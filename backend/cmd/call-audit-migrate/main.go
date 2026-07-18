package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
)

type commandMode string

const (
	modeMigrate        commandMode = "migrate"
	modePartitionsOnly commandMode = "partitions-only"
	modeCheck          commandMode = "check"
	modeRetention      commandMode = "retention-dry-run"
)

type commandOptions struct {
	mode          commandMode
	retentionDays int
}

type migrationStore interface {
	Ping(context.Context) error
	EnsureSchema(context.Context, time.Time) error
	EnsurePartitions(context.Context, time.Time) error
	CheckReadiness(context.Context, time.Time) error
	PreviewRetention(context.Context, time.Time) (callaudit.RetentionPreview, error)
}

func main() {
	options, err := parseCommandOptions(os.Args[1:], os.Stderr)
	if err != nil {
		fatal(err)
	}

	postgresURL := firstEnv("CALL_AUDIT_POSTGRES_URL", "AUDIT_POSTGRES_URL", "AUDIT_QUERY_MIGRATION_POSTGRES_URL")
	store, err := callaudit.OpenPostgreSQLStore(postgresURL, 2, 1)
	if err != nil {
		fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	result, err := executeCommand(ctx, store, options, time.Now())
	if err != nil {
		fatal(err)
	}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
}

func parseCommandOptions(args []string, output io.Writer) (commandOptions, error) {
	flags := flag.NewFlagSet("call-audit-migrate", flag.ContinueOnError)
	flags.SetOutput(output)
	check := flags.Bool("check", false, "read-only connectivity, schema, partition, and index readiness check")
	partitionsOnly := flags.Bool("partitions-only", false, "create only the current and next month partitions and indexes")
	retentionDryRun := flags.Bool("retention-dry-run", false, "read-only preview of expired calls and objects")
	retentionDays := flags.Int("retention-days", envInt("CALL_AUDIT_RETENTION_DAYS", envInt("AUDIT_RETENTION_DAYS", 180)), "retention cutoff in days")
	if err := flags.Parse(args); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	selected := 0
	mode := modeMigrate
	for _, candidate := range []struct {
		enabled bool
		mode    commandMode
	}{
		{enabled: *check, mode: modeCheck},
		{enabled: *partitionsOnly, mode: modePartitionsOnly},
		{enabled: *retentionDryRun, mode: modeRetention},
	} {
		if candidate.enabled {
			selected++
			mode = candidate.mode
		}
	}
	if selected > 1 {
		return commandOptions{}, fmt.Errorf("--check, --partitions-only, and --retention-dry-run are mutually exclusive")
	}
	return commandOptions{mode: mode, retentionDays: *retentionDays}, nil
}

func executeCommand(ctx context.Context, store migrationStore, options commandOptions, reference time.Time) (map[string]any, error) {
	if store == nil {
		return nil, fmt.Errorf("audit postgres store is required")
	}
	if reference.IsZero() {
		reference = time.Now()
	}
	if err := store.Ping(ctx); err != nil {
		return nil, err
	}

	result := map[string]any{}
	switch options.mode {
	case modeMigrate:
		if err := store.EnsureSchema(ctx, reference); err != nil {
			return nil, err
		}
		result["migrated"] = true
	case modePartitionsOnly:
		if err := store.EnsurePartitions(ctx, reference); err != nil {
			return nil, err
		}
		result["partitions_ensured"] = true
	case modeCheck:
		result["checked"] = true
	case modeRetention:
		if options.retentionDays <= 0 {
			return nil, fmt.Errorf("retention-days must be positive")
		}
		cutoff := reference.UTC().AddDate(0, 0, -options.retentionDays)
		preview, err := store.PreviewRetention(ctx, cutoff)
		if err != nil {
			return nil, err
		}
		result["retention_dry_run"] = true
		result["cutoff"] = cutoff
		result["preview"] = preview
	default:
		return nil, fmt.Errorf("unsupported call audit migration mode %q", options.mode)
	}
	if err := store.CheckReadiness(ctx, reference); err != nil {
		return nil, err
	}
	result["ready"] = true
	result["current_and_next_partitions"] = true
	return result, nil
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil {
		return fallback
	}
	return value
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "call-audit-migrate:", err)
	os.Exit(1)
}

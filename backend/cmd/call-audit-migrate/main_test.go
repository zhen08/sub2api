package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
)

type fakeMigrationStore struct {
	pingCalls       int
	schemaCalls     int
	partitionCalls  int
	readinessCalls  int
	retentionCalls  int
	retentionCutoff time.Time
}

func (s *fakeMigrationStore) Ping(context.Context) error {
	s.pingCalls++
	return nil
}

func (s *fakeMigrationStore) EnsureSchema(context.Context, time.Time) error {
	s.schemaCalls++
	return nil
}

func (s *fakeMigrationStore) EnsurePartitions(context.Context, time.Time) error {
	s.partitionCalls++
	return nil
}

func (s *fakeMigrationStore) CheckReadiness(context.Context, time.Time) error {
	s.readinessCalls++
	return nil
}

func (s *fakeMigrationStore) PreviewRetention(_ context.Context, cutoff time.Time) (callaudit.RetentionPreview, error) {
	s.retentionCalls++
	s.retentionCutoff = cutoff
	return callaudit.RetentionPreview{Calls: 2, Artifacts: 4}, nil
}

func TestExecuteCommandKeepsCheckReadOnlyAndSeparatesDDL(t *testing.T) {
	t.Parallel()
	reference := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		options        commandOptions
		wantSchema     int
		wantPartitions int
		wantRetention  int
		resultKey      string
	}{
		{name: "initial migration", options: commandOptions{mode: modeMigrate}, wantSchema: 1, resultKey: "migrated"},
		{name: "partitions only", options: commandOptions{mode: modePartitionsOnly}, wantPartitions: 1, resultKey: "partitions_ensured"},
		{name: "read only check", options: commandOptions{mode: modeCheck}, resultKey: "checked"},
		{name: "retention dry run", options: commandOptions{mode: modeRetention, retentionDays: 180}, wantRetention: 1, resultKey: "retention_dry_run"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := &fakeMigrationStore{}
			result, err := executeCommand(context.Background(), store, test.options, reference)
			if err != nil {
				t.Fatal(err)
			}
			if store.pingCalls != 1 || store.readinessCalls != 1 {
				t.Fatalf("ping/readiness calls = %d/%d", store.pingCalls, store.readinessCalls)
			}
			if store.schemaCalls != test.wantSchema || store.partitionCalls != test.wantPartitions || store.retentionCalls != test.wantRetention {
				t.Fatalf("schema/partitions/retention calls = %d/%d/%d", store.schemaCalls, store.partitionCalls, store.retentionCalls)
			}
			if result[test.resultKey] != true || result["ready"] != true {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestExecuteRetentionDryRunUsesReferenceCutoff(t *testing.T) {
	t.Parallel()
	reference := time.Date(2026, time.July, 18, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	store := &fakeMigrationStore{}
	if _, err := executeCommand(context.Background(), store, commandOptions{mode: modeRetention, retentionDays: 180}, reference); err != nil {
		t.Fatal(err)
	}
	want := reference.UTC().AddDate(0, 0, -180)
	if !store.retentionCutoff.Equal(want) {
		t.Fatalf("retention cutoff = %s, want %s", store.retentionCutoff, want)
	}
}

func TestParseCommandOptionsRejectsConflictingModes(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	if _, err := parseCommandOptions([]string{"--check", "--partitions-only"}, &output); err == nil {
		t.Fatal("expected conflicting modes to fail")
	}
	options, err := parseCommandOptions([]string{"--partitions-only"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if options.mode != modePartitionsOnly {
		t.Fatalf("mode = %q", options.mode)
	}
}

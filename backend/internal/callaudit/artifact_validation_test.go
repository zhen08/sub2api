package callaudit

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func replayTestBundle(t *testing.T) (*Spool, Manifest) {
	t.Helper()
	spool := newTestSpool(t)
	now := time.Date(2026, time.October, 25, 7, 0, 0, 0, time.UTC)
	ref, err := spool.WriteArtifact(now, "replay-validation", ArtifactResponse, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("replay-validation", now, ref)
	manifest.Version = 1
	return spool, manifest
}

func requireReplayError(t *testing.T, err error, code string, retryable bool) {
	t.Helper()
	var classified *ProcessError
	if !errors.As(err, &classified) || classified.Code != code || classified.Retryable != retryable {
		t.Fatalf("classification = %v, want code=%s retryable=%t", err, code, retryable)
	}
}

func TestValidateReplayBundleRejectsPermanentInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		code   string
		mutate func(*testing.T, *Spool, *Manifest)
	}{
		{"unsupported version", "manifest_version_unsupported", func(_ *testing.T, _ *Spool, m *Manifest) { m.Version = 2 }},
		{"missing identity", "invalid_manifest", func(_ *testing.T, _ *Spool, m *Manifest) { m.RequestID = "" }},
		{"invalid status", "invalid_manifest", func(_ *testing.T, _ *Spool, m *Manifest) { m.Status = "invalid" }},
		{"invalid kind", "invalid_manifest", func(_ *testing.T, _ *Spool, m *Manifest) { m.Artifacts[0].Kind = "invalid" }},
		{"duplicate tuple", "invalid_manifest", func(_ *testing.T, _ *Spool, m *Manifest) { m.Artifacts = append(m.Artifacts, m.Artifacts[0]) }},
		{"duplicate file", "artifact_duplicate_reference", func(_ *testing.T, _ *Spool, m *Manifest) {
			ref := m.Artifacts[0]
			ref.Sequence = 1
			m.Artifacts = append(m.Artifacts, ref)
		}},
		{"negative size", "artifact_invalid_reference", func(_ *testing.T, _ *Spool, m *Manifest) { m.Artifacts[0].Bytes = -1 }},
		{"escaping path", "artifact_unsafe_path", func(_ *testing.T, _ *Spool, m *Manifest) { m.Artifacts[0].SpoolPath = "../private-secret.json" }},
		{"queue file as artifact", "artifact_unsafe_path", func(_ *testing.T, _ *Spool, m *Manifest) { m.Artifacts[0].SpoolPath = "ready/private-secret.json" }},
		{"missing file", "artifact_missing", func(t *testing.T, s *Spool, m *Manifest) {
			if err := os.Remove(filepath.Join(s.root, m.Artifacts[0].SpoolPath)); err != nil {
				t.Fatal(err)
			}
		}},
		{"size mismatch", "artifact_size_mismatch", func(_ *testing.T, _ *Spool, m *Manifest) { m.Artifacts[0].Bytes++ }},
		{"oversized", "artifact_too_large", func(_ *testing.T, s *Spool, _ *Manifest) { s.maxArtifactBytes = 2 }},
		{"invalid JSON", "artifact_invalid_json", func(t *testing.T, s *Spool, m *Manifest) {
			raw := []byte(`{"private-secret":`)
			if err := os.WriteFile(filepath.Join(s.root, m.Artifacts[0].SpoolPath), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			m.Artifacts[0].Bytes = int64(len(raw))
		}},
		{"directory", "artifact_not_regular", func(t *testing.T, s *Spool, m *Manifest) {
			if err := os.Remove(filepath.Join(s.root, m.Artifacts[0].SpoolPath)); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(s.root, m.Artifacts[0].SpoolPath), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{"file symlink", "artifact_unsafe_path", func(t *testing.T, s *Spool, m *Manifest) {
			original := filepath.Join(s.root, m.Artifacts[0].SpoolPath)
			saved := filepath.Join(t.TempDir(), "private-secret.json")
			if err := os.Rename(original, saved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(saved, original); err != nil {
				t.Fatal(err)
			}
		}},
		{"directory symlink", "artifact_unsafe_path", func(t *testing.T, s *Spool, m *Manifest) {
			original := filepath.Dir(filepath.Join(s.root, m.Artifacts[0].SpoolPath))
			saved := filepath.Join(t.TempDir(), "private-secret-dir")
			if err := os.Rename(original, saved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(saved, original); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spool, manifest := replayTestBundle(t)
			test.mutate(t, spool, &manifest)
			err := validateReplayBundle(spool, manifest)
			requireReplayError(t, err, test.code, false)
			if strings.Contains(err.Error(), "private-secret") || strings.Contains(err.Error(), spool.root) {
				t.Fatal("validation exposed source details")
			}
		})
	}
}

func TestArtifactOwnershipBindsRequestDateKindAndSequence(t *testing.T) {
	t.Parallel()
	for _, mismatch := range []string{"request", "date", "kind", "sequence"} {
		t.Run(mismatch, func(t *testing.T) {
			spool, manifest := replayTestBundle(t)
			original := manifest.Artifacts[0]
			date, requestID, kind, sequence := manifest.CreatedAt, manifest.RequestID, original.Kind, original.Sequence
			switch mismatch {
			case "request":
				requestID = "another-request"
			case "date":
				date = date.Add(24 * time.Hour)
			case "kind":
				kind = ArtifactClientRequest
			case "sequence":
				sequence++
			}
			foreign, err := spool.WriteArtifact(date, requestID, kind, sequence, map[string]any{"ok": true})
			if err != nil {
				t.Fatal(err)
			}
			manifest.Artifacts[0].SpoolPath = foreign.SpoolPath
			for _, check := range []func(*Spool, Manifest) error{validateAuditBundle, validateReplayBundle} {
				requireReplayError(t, check(spool, manifest), "artifact_ownership_mismatch", false)
			}
			for _, ref := range []ArtifactRef{original, foreign} {
				if _, err := os.Stat(filepath.Join(spool.root, ref.SpoolPath)); err != nil {
					t.Fatalf("validation changed source: %v", err)
				}
			}
		})
	}
}

func TestCanonicalArtifactOwnershipSupportsUTCAndEncodedRequestNames(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	date := time.Date(2026, time.October, 25, 1, 0, 0, 0, time.FixedZone("offset", 8*3600))
	for _, id := range []string{"normal", "request/with spaces"} {
		ref, err := spool.WriteArtifactStream(date, id, ArtifactResponse, 2, func(w io.Writer) error { _, err := io.WriteString(w, `{"ok":true}`); return err })
		if err != nil {
			t.Fatal(err)
		}
		m := testManifest(id, date, ref)
		m.Version = 1
		if !strings.HasPrefix(ref.SpoolPath, "artifacts/2026-10-24/") {
			t.Fatalf("date is not UTC: %s", ref.SpoolPath)
		}
		if err := validateReplayBundle(spool, m); err != nil {
			t.Fatalf("writer-owned path rejected: %v", err)
		}
	}
}

func TestValidateReplayBundleAcceptsLegacyOptionalSize(t *testing.T) {
	t.Parallel()
	spool, manifest := replayTestBundle(t)
	if err := validateReplayBundle(spool, manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Artifacts[0].Bytes = 0
	if err := validateReplayBundle(spool, manifest); err != nil {
		t.Fatalf("legacy artifact without bytes: %v", err)
	}
	manifest.Artifacts = nil
	if err := validateReplayBundle(spool, manifest); err != nil {
		t.Fatalf("metadata-only capture: %v", err)
	}
}

func TestValidateAuditBundleDoesNotReparseOrdinaryArtifactBodies(t *testing.T) {
	t.Parallel()
	spool, manifest := replayTestBundle(t)
	raw := []byte(`{"private-secret":`)
	if err := os.WriteFile(filepath.Join(spool.root, manifest.Artifacts[0].SpoolPath), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest.Artifacts[0].Bytes = int64(len(raw))
	if err := validateAuditBundle(spool, manifest); err != nil {
		t.Fatalf("ordinary bundle validation read the artifact body: %v", err)
	}
	requireReplayError(t, validateReplayBundle(spool, manifest), "artifact_invalid_json", false)
	manifest.Artifacts[0].Bytes++
	requireReplayError(t, validateAuditBundle(spool, manifest), "artifact_size_mismatch", false)
}

func TestValidateArtifactJSONRequiresOneCompleteBoundedValue(t *testing.T) {
	t.Parallel()
	for _, input := range []string{`null`, `true`, `1e100000`, `"text"`, `[]`, `{}`, `{"a":[1,2,{"b":false}]}`, " \n{}\t "} {
		if err := validateArtifactJSON(strings.NewReader(input), int64(len(input))); err != nil {
			t.Fatalf("valid JSON rejected: %v", err)
		}
	}
	for _, input := range []string{"", " ", "{", "[", "[1,]", "{\"a\":}", "true false", "{}[]", "1e", "[true", "{\"a\":1"} {
		requireReplayError(t, validateArtifactJSON(strings.NewReader(input), 1024), "artifact_invalid_json", false)
	}
	requireReplayError(t, validateArtifactJSON(strings.NewReader(`{"a":"too long"}`), 4), "artifact_too_large", false)
}

type replayErrorReader struct{ err error }

func (r replayErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestReplayValidationKeepsRecoverableIOFailuresRetryableAndRedacted(t *testing.T) {
	t.Parallel()
	cause := &os.PathError{Op: "read", Path: "/private-secret", Err: os.ErrPermission}
	for _, err := range []error{
		replayArtifactIOError(cause),
		validateArtifactJSON(replayErrorReader{err: cause}, 1024),
	} {
		requireReplayError(t, err, "artifact_read_failed", true)
		if !errors.Is(err, os.ErrPermission) || strings.Contains(err.Error(), "private-secret") {
			t.Fatal("I/O classification lost its cause or exposed its path")
		}
	}
	if err := validateArtifactJSON(io.LimitReader(strings.NewReader(`{}`), 2), 2); err != nil {
		t.Fatal(err)
	}
}

func TestDurableProcessorRejectsMissingArtifactBeforeDatabaseOrS3(t *testing.T) {
	t.Parallel()
	spool, manifest := replayTestBundle(t)
	if err := os.Remove(filepath.Join(spool.root, manifest.Artifacts[0].SpoolPath)); err != nil {
		t.Fatal(err)
	}
	// Nil database/S3 clients would panic if validation did not precede remote
	// work. Neither remote dependency is needed to classify permanent damage.
	processor := &DurableProcessor{store: &PostgreSQLStore{}, objects: &ObjectStore{spool: spool}}
	requireReplayError(t, processor.Process(context.Background(), manifest), "artifact_missing", false)
}

func TestObjectStoreUploadPreservesPermanentMissingArtifact(t *testing.T) {
	t.Parallel()
	spool, manifest := replayTestBundle(t)
	if err := os.Remove(filepath.Join(spool.root, manifest.Artifacts[0].SpoolPath)); err != nil {
		t.Fatal(err)
	}
	objects := &ObjectStore{spool: spool, client: &s3.Client{}}
	_, err := objects.Upload(context.Background(), manifest, manifest.Artifacts[0])
	requireReplayError(t, err, "artifact_missing", false)
}

type replaySQLArgument func(driver.Value) bool

func (match replaySQLArgument) Match(value driver.Value) bool { return match(value) }

func TestDurableProcessorPreservesPermanentUploadFailure(t *testing.T) {
	t.Parallel()
	spool, manifest := replayTestBundle(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	args := make([]driver.Value, 27)
	for index := range args {
		args[index] = sqlmock.AnyArg()
	}
	var removeErr error
	args[0] = replaySQLArgument(func(value driver.Value) bool {
		// Simulate losing the source after preflight, while the initial metadata
		// write is in progress. Upload must retain the permanent classification.
		removeErr = os.Remove(filepath.Join(spool.root, manifest.Artifacts[0].SpoolPath))
		return value == manifest.RequestID && removeErr == nil
	})
	mock.ExpectExec(regexp.QuoteMeta(auditUpsertCallSQL)).WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))
	failedArgs := make([]driver.Value, 27)
	for index := range failedArgs {
		failedArgs[index] = sqlmock.AnyArg()
	}
	failedArgs[24] = string(CaptureFailed)
	failedArgs[25] = "audit bundle validation failed: artifact_missing"
	mock.ExpectExec(regexp.QuoteMeta(auditUpsertCallSQL)).WithArgs(failedArgs...).WillReturnResult(sqlmock.NewResult(1, 1))
	processor := &DurableProcessor{store: &PostgreSQLStore{db: db}, objects: &ObjectStore{spool: spool, client: &s3.Client{}}}
	requireReplayError(t, processor.Process(context.Background(), manifest), "artifact_missing", false)
	if removeErr != nil {
		t.Fatal(removeErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

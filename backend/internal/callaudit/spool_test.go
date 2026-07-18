package callaudit

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestSpool(t *testing.T) *Spool {
	t.Helper()
	spool, err := NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return spool
}

func testManifest(requestID string, createdAt time.Time, artifacts ...ArtifactRef) Manifest {
	return Manifest{
		RequestID:      requestID,
		CreatedAt:      createdAt,
		RetentionUntil: createdAt.Add(24 * time.Hour),
		Status:         CallStatusOK,
		Artifacts:      artifacts,
	}
}

func TestSpoolPublishesCompleteBundleAtomically(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	createdAt := time.Date(2026, time.July, 18, 7, 0, 0, 0, time.UTC)
	artifact, err := spool.WriteArtifact(createdAt, "req-1", ArtifactClientRequest, 0, map[string]any{"model": "claude"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := spool.ReadArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded["model"] != "claude" {
		t.Fatalf("artifact = %s, err = %v", raw, err)
	}

	manifestPath, err := spool.CommitManifest(testManifest("req-1", createdAt, artifact))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.State != ManifestReady || manifest.CaptureStatus != CapturePending || manifest.EventSpoolPath != manifestPath {
		t.Fatalf("manifest = %+v", manifest)
	}
	if _, err := spool.CommitManifest(testManifest("req-1", createdAt, artifact)); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("duplicate commit error = %v", err)
	}

	for _, relative := range []string{artifact.SpoolPath, manifestPath} {
		info, err := os.Stat(filepath.Join(spool.Root(), filepath.FromSlash(relative)))
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("%s mode = %#o", relative, got)
		}
	}
	for _, directory := range []string{spoolArtifactsDir, spoolReadyDir, spoolProcessingDir, spoolRetryDir, spoolDeadDir, spoolCompletedDir} {
		info, err := os.Stat(filepath.Join(spool.Root(), directory))
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("%s mode = %#o", directory, got)
		}
	}
}

func TestSpoolRejectsOversizedInvalidAndEscapingArtifacts(t *testing.T) {
	t.Parallel()
	spool, err := NewSpool(t.TempDir(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.WriteArtifact(time.Now(), "req", ArtifactResponse, 0, map[string]string{"large": "payload"}); err == nil {
		t.Fatal("expected size limit error")
	}
	if _, err := spool.WriteArtifact(time.Now(), "req", ArtifactKind("secret"), 0, nil); err == nil {
		t.Fatal("expected artifact kind validation error")
	}
	if _, err := spool.ReadArtifact(ArtifactRef{SpoolPath: "../../outside"}); err == nil {
		t.Fatal("expected path escape error")
	}
}

func TestSpoolCleanupOrphanTemps(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	orphan, err := spool.CreateCaptureTemp("request")
	if err != nil {
		t.Fatal(err)
	}
	orphanPath := orphan.Name()
	_ = orphan.Close()
	removed, err := spool.CleanupOrphanTemps()
	if err != nil || removed != 1 {
		t.Fatalf("CleanupOrphanTemps() = %d, %v", removed, err)
	}
	if _, err := os.Stat(orphanPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan still exists: %v", err)
	}
}

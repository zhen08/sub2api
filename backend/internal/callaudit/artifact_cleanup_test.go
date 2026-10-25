package callaudit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArtifactCleanupValidatesWholeBundleBeforeDeleting(t *testing.T) {
	t.Parallel()
	for _, completed := range []bool{false, true} {
		for _, mismatch := range []string{"request", "date", "kind", "sequence"} {
			name := mismatch
			if completed {
				name += "-completed"
			}
			t.Run(name, func(t *testing.T) {
				spool := newTestSpool(t)
				now := time.Now().UTC()
				own, err := spool.WriteArtifact(now, "owner", ArtifactClientRequest, 0, map[string]any{"owner": true})
				if err != nil {
					t.Fatal(err)
				}
				date, id, kind, seq := now, "owner", ArtifactResponse, 0
				switch mismatch {
				case "request":
					id = "other"
				case "date":
					date = date.Add(-24 * time.Hour)
				case "kind":
					kind = ArtifactUpstreamRequest
				case "sequence":
					seq = 1
				}
				foreign, err := spool.WriteArtifact(date, id, kind, seq, map[string]any{"other": true})
				if err != nil {
					t.Fatal(err)
				}
				badRef := foreign
				badRef.Kind = ArtifactResponse
				badRef.Sequence = 0
				manifest := testManifest("owner", now, own, badRef)
				manifest.Version = 1
				manifest.CaptureStatus = CaptureStored
				if completed {
					path := filepath.Join(spool.Root(), spoolCompletedDir, "owner-event.json")
					writeTestManifest(t, path, manifest)
					worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { return nil }), testWorkerOptions(&now))
					if err != nil {
						t.Fatal(err)
					}
					count, cleanupErr := worker.cleanupCompleted()
					requireReplayError(t, cleanupErr, "artifact_ownership_mismatch", false)
					if count != 0 {
						t.Fatalf("cleaned malformed completed bundle: %d", count)
					}
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("completed evidence removed: %v", err)
					}
				} else {
					requireReplayError(t, spool.removeBundle(manifest), "artifact_ownership_mismatch", false)
				}
				// The first ref is valid: checking one file at a time would delete
				// it before discovering the later foreign reference.
				for _, ref := range []ArtifactRef{own, foreign} {
					if _, err := os.Stat(filepath.Join(spool.Root(), ref.SpoolPath)); err != nil {
						t.Fatalf("source removed before whole-bundle validation: %v", err)
					}
				}
			})
		}
	}
}

func TestCompletedCleanupResumesAfterPartialDeletion(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	first, err := spool.WriteArtifact(now, "partial", ArtifactClientRequest, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	last, err := spool.WriteArtifact(now, "partial", ArtifactResponse, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("partial", now, first, last)
	manifest.Version = 1
	manifest.CaptureStatus = CaptureStored
	manifest.DeadLetterRetries = 1
	path := filepath.Join(spool.Root(), spoolCompletedDir, "partial-event.json")
	writeTestManifest(t, path, manifest)
	if err := os.Remove(filepath.Join(spool.Root(), first.SpoolPath)); err != nil {
		t.Fatal(err)
	}
	successes := 0
	options := testWorkerOptions(&now)
	options.OnSuccess = func(Manifest) { successes++ }
	worker, err := NewWorker(spool, ProcessorFunc(func(context.Context, Manifest) error { t.Fatal("cleanup replayed the processor"); return nil }), options)
	if err != nil {
		t.Fatal(err)
	}
	count, err := worker.cleanupCompleted()
	if err != nil || count != 1 || successes != 1 {
		t.Fatalf("partial cleanup did not resume: count=%d successes=%d err=%v", count, successes, err)
	}
	for _, p := range []string{path, filepath.Join(spool.Root(), last.SpoolPath)} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("successful cleanup retained file: %v", err)
		}
	}
	if count, err := worker.cleanupCompleted(); err != nil || count != 0 {
		t.Fatalf("cleanup not idempotent: %d %v", count, err)
	}
	if err := spool.removeBundle(manifest); err != nil {
		t.Fatalf("fully removed bundle rejected: %v", err)
	}
}

func TestArtifactCleanupRejectsSymlinkBeforeDeletingOwnedFile(t *testing.T) {
	t.Parallel()
	spool := newTestSpool(t)
	now := time.Now().UTC()
	first, err := spool.WriteArtifact(now, "symlink-cleanup", ArtifactClientRequest, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	last, err := spool.WriteArtifact(now, "symlink-cleanup", ArtifactResponse, 0, map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	lastPath := filepath.Join(spool.Root(), last.SpoolPath)
	foreign := filepath.Join(t.TempDir(), "foreign.json")
	if err := os.Rename(lastPath, foreign); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, lastPath); err != nil {
		t.Fatal(err)
	}
	manifest := testManifest("symlink-cleanup", now, first, last)
	requireReplayError(t, spool.removeBundle(manifest), "artifact_unsafe_path", false)
	for _, p := range []string{filepath.Join(spool.Root(), first.SpoolPath), foreign} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("symlink cleanup changed a source: %v", err)
		}
	}
}

package callaudit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	spoolArtifactsDir  = "artifacts"
	spoolReadyDir      = "ready"
	spoolProcessingDir = "processing"
	spoolRetryDir      = "retry"
	spoolDeadDir       = "dead-letter"
	spoolCompletedDir  = "completed"
	maxManifestBytes   = 4 << 20
)

var safeFilenamePart = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

type Spool struct {
	root             string
	maxArtifactBytes int64
}

func NewSpool(root string, maxArtifactBytes int64) (*Spool, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("audit spool directory is required")
	}
	if maxArtifactBytes <= 0 {
		return nil, fmt.Errorf("maximum artifact bytes must be positive")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve audit spool directory: %w", err)
	}
	spool := &Spool{root: absoluteRoot, maxArtifactBytes: maxArtifactBytes}
	if err := os.MkdirAll(absoluteRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create audit spool root: %w", err)
	}
	if err := os.Chmod(absoluteRoot, 0o700); err != nil {
		return nil, fmt.Errorf("secure audit spool root: %w", err)
	}
	for _, name := range []string{
		spoolArtifactsDir,
		spoolReadyDir,
		spoolProcessingDir,
		spoolRetryDir,
		spoolDeadDir,
		spoolCompletedDir,
	} {
		directory := filepath.Join(absoluteRoot, name)
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create audit spool %s directory: %w", name, err)
		}
		if err := os.Chmod(directory, 0o700); err != nil {
			return nil, fmt.Errorf("secure audit spool %s directory: %w", name, err)
		}
	}
	return spool, nil
}

func (s *Spool) Root() string { return s.root }

// CleanupOrphanTemps runs before the runtime accepts requests. At that point
// every dot-prefixed temporary file under artifacts belongs to a process that
// exited before publishing its artifact or manifest.
func (s *Spool) CleanupOrphanTemps() (int, error) {
	if s == nil {
		return 0, nil
	}
	root := filepath.Join(s.root, spoolArtifactsDir)
	removed := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".") {
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed++
		return nil
	})
	return removed, err
}

func (s *Spool) WriteArtifact(createdAt time.Time, requestID string, kind ArtifactKind, sequence int, payload any) (ArtifactRef, error) {
	if !kind.Valid() {
		return ArtifactRef{}, fmt.Errorf("invalid artifact kind %q", kind)
	}
	if sequence < 0 {
		return ArtifactRef{}, fmt.Errorf("artifact sequence must be non-negative")
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	raw, err := marshalArtifact(payload)
	if err != nil {
		return ArtifactRef{}, err
	}
	if int64(len(raw)) > s.maxArtifactBytes {
		return ArtifactRef{}, fmt.Errorf("artifact exceeds %d byte limit", s.maxArtifactBytes)
	}
	filename := fmt.Sprintf("%s-%s-%d.json", spoolRequestFilePart(requestID), kind, sequence)
	relative := filepath.Join(spoolArtifactsDir, createdAt.UTC().Format(time.DateOnly), filename)
	absolute, err := s.safePath(relative)
	if err != nil {
		return ArtifactRef{}, err
	}
	if err := atomicWriteFile(absolute, raw, 0o600, false); err != nil {
		return ArtifactRef{}, err
	}
	return ArtifactRef{
		Kind:        kind,
		Sequence:    sequence,
		SpoolPath:   filepath.ToSlash(relative),
		ContentType: "application/json",
		Bytes:       int64(len(raw)),
	}, nil
}

// WriteArtifactStream publishes an artifact without first materializing it in
// memory. The writer is bounded by maxArtifactBytes and the final path is still
// made visible with an fsync + atomic rename.
func (s *Spool) WriteArtifactStream(createdAt time.Time, requestID string, kind ArtifactKind, sequence int, write func(io.Writer) error) (ArtifactRef, error) {
	if !kind.Valid() {
		return ArtifactRef{}, fmt.Errorf("invalid artifact kind %q", kind)
	}
	if sequence < 0 || write == nil {
		return ArtifactRef{}, fmt.Errorf("invalid streaming artifact input")
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	filename := fmt.Sprintf("%s-%s-%d.json", spoolRequestFilePart(requestID), kind, sequence)
	relative := filepath.Join(spoolArtifactsDir, createdAt.UTC().Format(time.DateOnly), filename)
	absolute, err := s.safePath(relative)
	if err != nil {
		return ArtifactRef{}, err
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return ArtifactRef{}, err
	}
	temp, err := os.CreateTemp(filepath.Dir(absolute), ".callaudit-stream-*.tmp")
	if err != nil {
		return ArtifactRef{}, err
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		_ = temp.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return ArtifactRef{}, err
	}
	bounded := &artifactLimitWriter{writer: temp, remaining: s.maxArtifactBytes}
	if err := write(bounded); err != nil {
		return ArtifactRef{}, err
	}
	if err := temp.Sync(); err != nil {
		return ArtifactRef{}, err
	}
	if err := temp.Close(); err != nil {
		return ArtifactRef{}, err
	}
	if err := os.Rename(tempPath, absolute); err != nil {
		return ArtifactRef{}, err
	}
	removeTemp = false
	if err := syncDirectory(filepath.Dir(absolute)); err != nil {
		return ArtifactRef{}, err
	}
	return ArtifactRef{Kind: kind, Sequence: sequence, SpoolPath: filepath.ToSlash(relative), ContentType: "application/json", Bytes: bounded.written}, nil
}

func (s *Spool) CreateCaptureTemp(label string) (*os.File, error) {
	directory := filepath.Join(s.root, spoolArtifactsDir, "inflight")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	label = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, label)
	if label == "" {
		label = "capture"
	}
	file, err := os.CreateTemp(directory, "."+label+"-*.tmp")
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	return file, nil
}

type artifactLimitWriter struct {
	writer    io.Writer
	remaining int64
	written   int64
}

func (w *artifactLimitWriter) Write(payload []byte) (int, error) {
	if int64(len(payload)) > w.remaining {
		return 0, fmt.Errorf("artifact exceeds configured byte limit")
	}
	n, err := w.writer.Write(payload)
	w.remaining -= int64(n)
	w.written += int64(n)
	return n, err
}

func marshalArtifact(payload any) ([]byte, error) {
	if raw, ok := payload.(json.RawMessage); ok {
		if !json.Valid(raw) {
			return nil, fmt.Errorf("artifact payload is not valid JSON")
		}
		return append([]byte(nil), raw...), nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal audit artifact: %w", err)
	}
	return raw, nil
}

// CommitManifest is the queue commit point. Artifacts are written first; the
// exclusive, atomically published ready manifest makes the bundle visible to a worker.
func (s *Spool) CommitManifest(manifest Manifest) (string, error) {
	if err := validateManifest(manifest); err != nil {
		return "", err
	}
	manifest.Version = 1
	manifest.State = ManifestReady
	manifest.CaptureStatus = CapturePending
	manifest.Attempts = 0
	manifest.NextAttemptAt = nil
	manifest.ClaimedAt = nil
	filename := spoolRequestFilePart(manifest.RequestID) + "-event.json"
	relative := filepath.Join(spoolReadyDir, filename)
	manifest.EventSpoolPath = filepath.ToSlash(relative)
	raw, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("marshal audit manifest: %w", err)
	}
	absolute, err := s.safePath(relative)
	if err != nil {
		return "", err
	}
	if err := atomicWriteFile(absolute, raw, 0o600, true); err != nil {
		return "", err
	}
	return filepath.ToSlash(relative), nil
}

func validateManifest(manifest Manifest) error {
	if strings.TrimSpace(manifest.RequestID) == "" {
		return fmt.Errorf("manifest request id is required")
	}
	if manifest.CreatedAt.IsZero() {
		return fmt.Errorf("manifest created time is required")
	}
	if manifest.RetentionUntil.IsZero() {
		return fmt.Errorf("manifest retention time is required")
	}
	if manifest.Status == "" {
		return fmt.Errorf("manifest call status is required")
	}
	seen := make(map[string]struct{}, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		if !artifact.Kind.Valid() || artifact.Sequence < 0 || artifact.SpoolPath == "" {
			return fmt.Errorf("manifest contains invalid artifact reference")
		}
		key := fmt.Sprintf("%s:%d", artifact.Kind, artifact.Sequence)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("manifest contains duplicate artifact %s", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func (s *Spool) LoadManifest(relativeOrAbsolute string) (Manifest, error) {
	absolute, err := s.safePath(relativeOrAbsolute)
	if err != nil {
		return Manifest{}, err
	}
	file, err := os.Open(absolute)
	if err != nil {
		return Manifest{}, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return Manifest{}, err
	}
	if len(raw) > maxManifestBytes {
		return Manifest{}, fmt.Errorf("audit manifest exceeds %d byte limit", maxManifestBytes)
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode audit manifest: %w", err)
	}
	return manifest, nil
}

func (s *Spool) ReadArtifact(ref ArtifactRef) ([]byte, error) {
	absolute, err := s.safePath(ref.SpoolPath)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(absolute)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, s.maxArtifactBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > s.maxArtifactBytes {
		return nil, fmt.Errorf("artifact exceeds %d byte limit", s.maxArtifactBytes)
	}
	return raw, nil
}

func (s *Spool) listManifestPaths(directory string) ([]string, error) {
	dir := filepath.Join(s.root, directory)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), "-event.json") {
			paths = append(paths, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func (s *Spool) safePath(relativeOrAbsolute string) (string, error) {
	candidate := relativeOrAbsolute
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(s.root, filepath.FromSlash(candidate))
	}
	candidate = filepath.Clean(candidate)
	relative, err := filepath.Rel(s.root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("audit spool path escapes root")
	}
	return candidate, nil
}

func (s *Spool) removeBundle(manifest Manifest) error {
	var joined error
	for _, artifact := range manifest.Artifacts {
		absolute, err := s.safePath(artifact.SpoolPath)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if err := os.Remove(absolute); err != nil && !errors.Is(err, os.ErrNotExist) {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func spoolRequestFilePart(requestID string) string {
	trimmed := strings.TrimSpace(requestID)
	if trimmed != "" && len(trimmed) <= 180 && safeFilenamePart.MatchString(trimmed) {
		return trimmed
	}
	sum := sha256.Sum256([]byte(requestID))
	return "request-" + hex.EncodeToString(sum[:16])
}

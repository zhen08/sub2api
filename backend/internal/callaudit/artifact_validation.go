package callaudit

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// validateReplayBundle fully validates a due dead letter before it can
// overwrite deterministic S3 objects. Normal capture already writes valid JSON
// and uses validateAuditBundle to avoid parsing every artifact a second time.
func validateReplayBundle(spool *Spool, manifest Manifest) error {
	return validateAuditBundleInputs(spool, manifest, true)
}

// validateAuditBundle performs inexpensive structural and filesystem checks
// before database/S3 work, without reading ordinary request/response bodies.
func validateAuditBundle(spool *Spool, manifest Manifest) error {
	return validateAuditBundleInputs(spool, manifest, false)
}

func validateAuditBundleInputs(spool *Spool, manifest Manifest, fullJSON bool) error {
	if spool == nil {
		return replayValidationError("artifact_spool_unavailable", true, nil)
	}
	if manifest.Version != 1 {
		return replayValidationError("manifest_version_unsupported", false, nil)
	}
	if err := validateManifest(manifest); err != nil {
		return replayValidationError("invalid_manifest", false, err)
	}
	switch manifest.Status {
	case CallStatusOK, CallStatusError, CallStatusAborted:
	default:
		return replayValidationError("invalid_manifest", false, nil)
	}
	if err := validateBundleArtifactOwnership(spool, manifest); err != nil {
		return err
	}
	for _, ref := range manifest.Artifacts {
		file, err := openReplayArtifact(spool, ref)
		if err != nil {
			return err
		}
		if fullJSON {
			err = validateArtifactJSON(file, spool.maxArtifactBytes)
		}
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return replayValidationError("artifact_read_failed", true, closeErr)
		}
	}
	return nil
}

// Ownership is checked for the entire bundle before any read, upload or
// cleanup. A syntactically safe path can still belong to a different request.
// This check deliberately performs no filesystem reads, so cleanup remains
// resumable after some of the correctly owned files have already been removed.
func validateBundleArtifactOwnership(spool *Spool, manifest Manifest) error {
	seen := make(map[string]struct{}, len(manifest.Artifacts))
	for _, ref := range manifest.Artifacts {
		path, err := replayArtifactPath(spool, ref)
		if err != nil {
			return err
		}
		if _, exists := seen[path]; exists {
			return replayValidationError("artifact_duplicate_reference", false, nil)
		}
		seen[path] = struct{}{}
		if err := validateArtifactOwnership(manifest, ref); err != nil {
			return err
		}
	}
	return nil
}

func validateArtifactOwnership(manifest Manifest, ref ArtifactRef) error {
	if strings.TrimSpace(manifest.RequestID) == "" || manifest.CreatedAt.IsZero() || !ref.Kind.Valid() || ref.Sequence < 0 {
		return replayValidationError("invalid_manifest", false, nil)
	}
	if ref.SpoolPath != canonicalArtifactSpoolPath(manifest.CreatedAt, manifest.RequestID, ref.Kind, ref.Sequence) {
		return replayValidationError("artifact_ownership_mismatch", false, nil)
	}
	return nil
}

// Completed bundles may be partially cleaned already. Missing canonical paths
// are harmless, while symlinks and unexpected file types must stop all cleanup.
func validateCleanupArtifactPath(spool *Spool, ref ArtifactRef) error {
	path, err := replayArtifactPath(spool, ref)
	if err != nil {
		return err
	}
	relative, _ := filepath.Rel(spool.root, path)
	components := append([]string{""}, strings.Split(relative, string(filepath.Separator))...)
	current := spool.root
	for index, component := range components {
		if component != "" {
			current = filepath.Join(current, component)
		}
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return replayArtifactIOError(err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return replayValidationError("artifact_unsafe_path", false, nil)
		}
		if index < len(components)-1 {
			if !info.IsDir() {
				return replayValidationError("artifact_unsafe_path", false, nil)
			}
		} else if !info.Mode().IsRegular() {
			return replayValidationError("artifact_not_regular", false, nil)
		}
	}
	return nil
}

func replayArtifactPath(spool *Spool, ref ArtifactRef) (string, error) {
	if !ref.Kind.Valid() || ref.Sequence < 0 || ref.Bytes < 0 || ref.SpoolPath == "" {
		return "", replayValidationError("artifact_invalid_reference", false, nil)
	}
	path, err := spool.safePath(ref.SpoolPath)
	if err != nil {
		return "", replayValidationError("artifact_unsafe_path", false, err)
	}
	relative, err := filepath.Rel(spool.root, path)
	if err != nil || !strings.HasPrefix(relative, spoolArtifactsDir+string(filepath.Separator)) {
		return "", replayValidationError("artifact_unsafe_path", false, err)
	}
	return path, nil
}

// openReplayArtifact rejects symbolic links in every path component. The
// descriptor identity is checked again after opening so replacing the final
// file between Lstat and Open cannot silently change the source being uploaded.
func openReplayArtifact(spool *Spool, ref ArtifactRef) (*os.File, error) {
	path, err := replayArtifactPath(spool, ref)
	if err != nil {
		return nil, err
	}
	relative, _ := filepath.Rel(spool.root, path)
	components := append([]string{""}, strings.Split(relative, string(filepath.Separator))...)
	current := spool.root
	var checked os.FileInfo
	for index, component := range components {
		if component != "" {
			current = filepath.Join(current, component)
		}
		checked, err = os.Lstat(current)
		if err != nil {
			return nil, replayArtifactIOError(err)
		}
		if checked.Mode()&os.ModeSymlink != 0 {
			return nil, replayValidationError("artifact_unsafe_path", false, nil)
		}
		if index < len(components)-1 && !checked.IsDir() {
			return nil, replayValidationError("artifact_unsafe_path", false, nil)
		}
	}
	if !checked.Mode().IsRegular() {
		return nil, replayValidationError("artifact_not_regular", false, nil)
	}
	if checked.Size() > spool.maxArtifactBytes {
		return nil, replayValidationError("artifact_too_large", false, nil)
	}
	// Bytes is optional in the legacy format. A nonzero value is an integrity
	// assertion, while a legacy missing value still receives the bounded read.
	if ref.Bytes > 0 && checked.Size() != ref.Bytes {
		return nil, replayValidationError("artifact_size_mismatch", false, nil)
	}
	root, err := os.OpenRoot(spool.root)
	if err != nil {
		return nil, replayArtifactIOError(err)
	}
	defer root.Close()
	file, err := root.Open(relative)
	if err != nil {
		return nil, replayArtifactIOError(err)
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, replayArtifactIOError(err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(checked, opened) {
		_ = file.Close()
		return nil, replayValidationError("artifact_changed", false, nil)
	}
	return file, nil
}

// Tokens discard each value as it is validated instead of retaining a decoded
// request/response tree. The reader remains bounded even if a file grows after
// the initial size check. UseNumber avoids rejecting valid large JSON numbers.
func validateArtifactJSON(reader io.Reader, maximum int64) error {
	limited := &io.LimitedReader{R: reader, N: maximum + 1}
	decoder := json.NewDecoder(limited)
	decoder.UseNumber()
	depth := 0
	started, complete := false, false
	for {
		token, err := decoder.Token()
		if limited.N == 0 {
			return replayValidationError("artifact_too_large", false, nil)
		}
		if errors.Is(err, io.EOF) {
			if started && complete {
				return nil
			}
			return replayValidationError("artifact_invalid_json", false, nil)
		}
		if err != nil {
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) || errors.Is(err, io.ErrUnexpectedEOF) {
				return replayValidationError("artifact_invalid_json", false, err)
			}
			return replayValidationError("artifact_read_failed", true, err)
		}
		if complete {
			return replayValidationError("artifact_invalid_json", false, nil)
		}
		started = true
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
				if depth > 10000 {
					return replayValidationError("artifact_invalid_json", false, nil)
				}
			case '}', ']':
				depth--
			}
		}
		complete = depth == 0
	}
}

func replayArtifactIOError(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return replayValidationError("artifact_missing", false, err)
	}
	return replayValidationError("artifact_read_failed", true, err)
}

// Preserve the underlying error for errors.Is without exposing file paths,
// parser fragments, or filesystem/driver details through Error().
type replayValidationCause struct {
	code  string
	cause error
}

func (e *replayValidationCause) Error() string { return "audit bundle validation failed: " + e.code }
func (e *replayValidationCause) Unwrap() error { return e.cause }

func replayValidationError(code string, retryable bool, cause error) error {
	err := &replayValidationCause{code: code, cause: cause}
	if retryable {
		return Retryable(code, err)
	}
	return Permanent(code, err)
}

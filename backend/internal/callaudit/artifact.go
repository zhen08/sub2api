package callaudit

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

func BuildLegacyObjectKey(prefix string, createdAt time.Time, apiKeyID, requestID string, kind ArtifactKind, sequence int) (string, error) {
	if !kind.Valid() {
		return "", fmt.Errorf("invalid artifact kind %q", kind)
	}
	if sequence < 0 {
		return "", fmt.Errorf("artifact sequence must be non-negative")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		prefix = LegacyObjectPrefix
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	filename := encodeURIComponent(string(kind)) + ".json.gz"
	if sequence > 0 {
		filename = fmt.Sprintf("%s-%d.json.gz", encodeURIComponent(string(kind)), sequence)
	}
	return strings.Join([]string{
		prefix,
		"dt=" + createdAt.UTC().Format(time.DateOnly),
		"api_key=" + encodePathPart(apiKeyID),
		"request_id=" + encodePathPart(requestID),
		filename,
	}, "/"), nil
}

func LegacyS3Metadata(requestID string, kind ArtifactKind) map[string]string {
	return map[string]string{
		S3MetadataRequestID:    requestID,
		S3MetadataArtifactKind: string(kind),
	}
}

func GzipArtifact(raw []byte) (compressed []byte, sha256Hex string, err error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err = writer.Write(raw); err != nil {
		return nil, "", err
	}
	if err = writer.Close(); err != nil {
		return nil, "", err
	}
	compressed = buffer.Bytes()
	sum := sha256.Sum256(compressed)
	return compressed, hex.EncodeToString(sum[:]), nil
}

func encodePathPart(value string) string {
	if value == "" {
		return "unknown"
	}
	return encodeURIComponent(value)
}

// encodeURIComponent matches JavaScript encodeURIComponent, which was used by
// claude-relay-service to build the historical MinIO object keys.
func encodeURIComponent(value string) string {
	const hexChars = "0123456789ABCDEF"
	var builder strings.Builder
	for _, b := range []byte(value) {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') ||
			strings.ContainsRune("-_.!~*'()", rune(b)) {
			builder.WriteByte(b)
			continue
		}
		builder.WriteByte('%')
		builder.WriteByte(hexChars[b>>4])
		builder.WriteByte(hexChars[b&0x0f])
	}
	return builder.String()
}

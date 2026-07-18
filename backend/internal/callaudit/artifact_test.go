package callaudit

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
	"time"
)

func TestBuildLegacyObjectKey(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, time.July, 18, 15, 30, 0, 0, time.FixedZone("CST", 8*60*60))
	key, err := BuildLegacyObjectKey("/custom-prefix/", createdAt, "api key/\u4f60\u597d", "req?x=1", ArtifactUpstreamRequest, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := "custom-prefix/dt=2026-07-18/api_key=api%20key%2F%E4%BD%A0%E5%A5%BD/request_id=req%3Fx%3D1/upstream_request.json.gz"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}

	key, err = BuildLegacyObjectKey("", createdAt, "", "request", ArtifactResponse, 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := "ai-call-audit/dt=2026-07-18/api_key=unknown/request_id=request/response-2.json.gz"; key != want {
		t.Fatalf("sequenced key = %q, want %q", key, want)
	}
}

func TestGzipArtifactAndLegacyMetadata(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"model":"claude","secret":false}`)
	compressed, digest, err := GzipArtifact(raw)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(compressed)
	if got := hex.EncodeToString(sum[:]); got != digest {
		t.Fatalf("digest = %q, want %q", digest, got)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	decompressed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decompressed, raw) {
		t.Fatalf("decompressed = %q", decompressed)
	}
	metadata := LegacyS3Metadata("req-1", ArtifactResponse)
	if metadata[S3MetadataRequestID] != "req-1" || metadata[S3MetadataArtifactKind] != "response" {
		t.Fatalf("metadata = %#v", metadata)
	}
}

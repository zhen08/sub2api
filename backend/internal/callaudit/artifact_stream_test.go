package callaudit

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestWriteArtifactEnvelopeStreamsJSONBody(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	err := WriteArtifactEnvelope(&output, map[string]any{"kind": "client_request"}, strings.NewReader(`{"model":"claude"}`), BodyJSON)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("artifact is invalid JSON: %v\n%s", err, output.String())
	}
	body, ok := decoded["body"].(map[string]any)
	if !ok || body["model"] != "claude" {
		t.Fatalf("body = %#v", decoded["body"])
	}
}

func TestWriteArtifactEnvelopeEscapesSSEResponse(t *testing.T) {
	t.Parallel()
	input := "event: message\ndata: {\"text\":\"你好\"}\n\n"
	var output bytes.Buffer
	if err := WriteArtifactEnvelope(&output, map[string]any{"kind": "response"}, strings.NewReader(input), BodyUTF8Raw); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Body struct {
			Raw      string `json:"raw"`
			Encoding string `json:"encoding"`
		} `json:"body"`
	}
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("artifact is invalid JSON: %v\n%s", err, output.String())
	}
	if decoded.Body.Raw != input || decoded.Body.Encoding != "utf8" {
		t.Fatalf("body = %#v", decoded.Body)
	}
}

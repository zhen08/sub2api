package callaudit

import "testing"

func TestClassifyRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		method   string
		target   string
		eligible bool
		protocol Protocol
	}{
		{name: "anthropic canonical", method: "POST", target: "/v1/messages?beta=true", eligible: true, protocol: ProtocolAnthropic},
		{name: "anthropic legacy", method: "post", target: "/claude/v1/messages", eligible: true, protocol: ProtocolAnthropic},
		{name: "openai legacy", method: "POST", target: "/openai/v1/chat/completions", eligible: true, protocol: ProtocolOpenAI},
		{name: "legacy completions", method: "POST", target: "/api/v1/completions", eligible: true, protocol: ProtocolAnthropic},
		{name: "responses operation", method: "POST", target: "/v1/responses/resp_1/cancel", eligible: true, protocol: ProtocolOpenAI},
		{name: "codex direct responses", method: "POST", target: "/backend-api/codex/responses", eligible: true, protocol: ProtocolOpenAI},
		{name: "gemini cli", method: "POST", target: "/gemini-cli/api/v1/models/gemini:streamGenerateContent", eligible: true, protocol: ProtocolGeminiCLI},
		{name: "antigravity", method: "POST", target: "/antigravity/api/v1/chat/completions", eligible: true, protocol: ProtocolAntigravity},
		{name: "count tokens excluded", method: "POST", target: "/api/v1/messages/count_tokens", eligible: false, protocol: ProtocolAnthropic},
		{name: "models excluded", method: "POST", target: "/openai/v1/models", eligible: false, protocol: ProtocolOpenAI},
		{name: "admin excluded", method: "POST", target: "/api/v1/admin/chat/completions", eligible: false, protocol: ProtocolAnthropic},
		{name: "read excluded", method: "GET", target: "/v1/messages", eligible: false, protocol: ProtocolAnthropic},
		{name: "unknown post", method: "POST", target: "/api/v1/ping", eligible: false, protocol: ProtocolAnthropic},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := ClassifyRoute(test.method, test.target)
			if got.Eligible != test.eligible || got.Protocol != test.protocol {
				t.Fatalf("ClassifyRoute(%q, %q) = %+v, want eligible=%v protocol=%q", test.method, test.target, got, test.eligible, test.protocol)
			}
		})
	}
}

func TestNormalizePath(t *testing.T) {
	t.Parallel()
	if got := NormalizePath(" openai//v1/../v1/CHAT/completions?key=secret "); got != "/openai/v1/chat/completions" {
		t.Fatalf("NormalizePath() = %q", got)
	}
}

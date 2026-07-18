package middleware

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestExtractAPIKeyForGatewayCRSCompatibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{name: "x api key", headers: map[string]string{"x-api-key": "x-key"}, want: "x-key"},
		{name: "google key", headers: map[string]string{"x-goog-api-key": "goog-key"}, want: "goog-key"},
		{name: "bearer", headers: map[string]string{"Authorization": "Bearer bearer-key"}, want: "bearer-key"},
		{name: "bare authorization", headers: map[string]string{"Authorization": "bare-key"}, want: "bare-key"},
		{name: "api key", headers: map[string]string{"api-key": "legacy-key"}, want: "legacy-key"},
		{
			name: "legacy priority",
			headers: map[string]string{
				"x-api-key":      "x-key",
				"x-goog-api-key": "goog-key",
				"Authorization":  "Bearer bearer-key",
				"api-key":        "legacy-key",
			},
			want: "x-key",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			context := &gin.Context{Request: httptest.NewRequest("POST", "/api/v1/messages", nil)}
			for key, value := range test.headers {
				context.Request.Header.Set(key, value)
			}
			if got := extractAPIKeyForGateway(context); got != test.want {
				t.Fatalf("extractAPIKeyForGateway() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestExtractAPIKeyForGoogleUsesCRSHeaderContractBeforeQueryFallback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		headers map[string]string
		target  string
		want    string
	}{
		{name: "legacy api key header", headers: map[string]string{"api-key": "legacy-key"}, target: "/v1beta/models/x:generateContent", want: "legacy-key"},
		{name: "bare authorization", headers: map[string]string{"Authorization": "bare-key"}, target: "/v1beta/models/x:generateContent", want: "bare-key"},
		{
			name: "CRS header priority",
			headers: map[string]string{
				"x-api-key":      "x-key",
				"x-goog-api-key": "goog-key",
				"Authorization":  "Bearer bearer-key",
				"api-key":        "legacy-key",
			},
			target: "/v1beta/models/x:generateContent?key=query-key",
			want:   "x-key",
		},
		{name: "native query fallback", target: "/v1beta/models/x:generateContent?key=query-key", want: "query-key"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			context := &gin.Context{Request: httptest.NewRequest("POST", test.target, nil)}
			for key, value := range test.headers {
				context.Request.Header.Set(key, value)
			}
			if got := extractAPIKeyForGoogle(context); got != test.want {
				t.Fatalf("extractAPIKeyForGoogle() = %q, want %q", got, test.want)
			}
		})
	}
}

package callaudit

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

func TestRedactHeadersDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	headers := http.Header{
		"Authorization":        {"Bearer secret"},
		"X-Goog-Api-Key":       {"gemini-secret"},
		"X-Amz-Security-Token": {"aws-session-secret"},
		"X-Relay-Token":        {"custom-secret"},
		"X-Request-Trace":      {"one", "two"},
	}
	original := headers.Clone()
	redacted := RedactHeaders(headers)

	if got := redacted.Get("authorization"); got != RedactedValue {
		t.Fatalf("Authorization = %q", got)
	}
	if got := redacted.Get("x-goog-api-key"); got != RedactedValue {
		t.Fatalf("X-Goog-Api-Key = %q", got)
	}
	if got := redacted.Get("x-amz-security-token"); got != RedactedValue {
		t.Fatalf("X-Amz-Security-Token = %q", got)
	}
	if got := redacted.Get("x-relay-token"); got != RedactedValue {
		t.Fatalf("X-Relay-Token = %q", got)
	}
	if !reflect.DeepEqual(redacted.Values("X-Request-Trace"), []string{"one", "two"}) {
		t.Fatalf("safe header changed: %#v", redacted)
	}
	redacted["X-Request-Trace"][0] = "changed"
	if !reflect.DeepEqual(headers, original) {
		t.Fatalf("input headers mutated: %#v", headers)
	}
}

func TestAuditHeaderAndQueryWireShape(t *testing.T) {
	t.Parallel()
	headers := AuditHeaders(http.Header{
		"X-Safe":          {"value"},
		"X-Multi":         {"one", "two"},
		"X-Custom-Secret": {"do-not-store"},
	})
	if headers["x-safe"] != "value" || !reflect.DeepEqual(headers["x-multi"], []string{"one", "two"}) || headers["x-custom-secret"] != RedactedValue {
		t.Fatalf("AuditHeaders() = %#v", headers)
	}
	query := AuditQuery(url.Values{"alt": {"sse"}, "X-Amz-Credential": {"secret"}})
	if query["alt"] != "sse" || query["X-Amz-Credential"] != RedactedValue {
		t.Fatalf("AuditQuery() = %#v", query)
	}
}

func TestRedactQueryDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	query := url.Values{"API_KEY": {"secret", "secret-2"}, "model": {"claude"}}
	redacted := RedactQuery(query)
	if !reflect.DeepEqual(redacted["API_KEY"], []string{RedactedValue}) {
		t.Fatalf("API_KEY was not redacted: %#v", redacted)
	}
	redacted["model"][0] = "changed"
	if got := query.Get("model"); got != "claude" {
		t.Fatalf("input query mutated: %q", got)
	}
}

func TestRedactURLRemovesCredentialsAndSensitiveQuery(t *testing.T) {
	t.Parallel()
	parsed, err := url.Parse("https://user:pass@example.com/v1/models?key=secret&alt=sse")
	if err != nil {
		t.Fatal(err)
	}
	got := RedactURL(parsed)
	if got != "https://example.com/v1/models?alt=sse&key=%5BREDACTED%5D" {
		t.Fatalf("RedactURL() = %q", got)
	}
}

func TestRedactionRejectsCompactAndCamelCaseCredentialNames(t *testing.T) {
	t.Parallel()
	headers := AuditHeaders(http.Header{
		"X-ApiKey":       {"header-secret"},
		"X-AccessToken":  {"access-secret"},
		"X-ClientSecret": {"client-secret"},
		"X-Monkey":       {"not-a-secret-name"},
	})
	for _, key := range []string{"x-apikey", "x-accesstoken", "x-clientsecret"} {
		if headers[key] != RedactedValue {
			t.Fatalf("header %s was not redacted: %#v", key, headers[key])
		}
	}
	if headers["x-monkey"] != "not-a-secret-name" {
		t.Fatalf("unrelated header was over-redacted: %#v", headers["x-monkey"])
	}

	query := AuditQuery(url.Values{
		"apiKey":       {"query-secret"},
		"accessToken":  {"access-secret"},
		"clientSecret": {"client-secret"},
		"key[]":        {"array-secret"},
	})
	for _, key := range []string{"apiKey", "accessToken", "clientSecret", "key[]"} {
		if query[key] != RedactedValue {
			t.Fatalf("query %s was not redacted: %#v", key, query[key])
		}
	}
}

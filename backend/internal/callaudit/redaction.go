package callaudit

import (
	"net/http"
	"net/url"
	"strings"
)

const RedactedValue = "[REDACTED]"

var sensitiveHeaders = map[string]struct{}{
	"authorization":       {},
	"proxy-authorization": {},
	"x-api-key":           {},
	"x-goog-api-key":      {},
	"api-key":             {},
	"x-auth-token":        {},
	"cookie":              {},
	"set-cookie":          {},
}

var sensitiveQueryKeys = map[string]struct{}{
	"key":          {},
	"api_key":      {},
	"api-key":      {},
	"access_token": {},
	"token":        {},
	"signature":    {},
	"sig":          {},
}

var sensitiveNameParts = map[string]struct{}{
	"auth":       {},
	"credential": {},
	"key":        {},
	"password":   {},
	"secret":     {},
	"signature":  {},
	"token":      {},
}

var compactSensitiveNames = []string{
	"apikey",
	"accesstoken",
	"authtoken",
	"bearertoken",
	"clientsecret",
	"credential",
	"password",
	"signature",
}

func isSensitiveName(name string, exact map[string]struct{}) bool {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if _, sensitive := exact[normalized]; sensitive {
		return true
	}
	parts := strings.FieldsFunc(normalized, func(r rune) bool {
		return r == '-' || r == '_' || r == '.'
	})
	for _, part := range parts {
		if _, sensitive := sensitiveNameParts[part]; sensitive {
			return true
		}
	}
	// Header canonicalization and query parsers do not preserve camelCase word
	// boundaries. Compact matching closes apiKey/accessToken/clientSecret and
	// array-style key[] bypasses without redacting unrelated words such as
	// "monkey" merely because they end in the letters "key".
	compact := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return -1
	}, normalized)
	switch compact {
	case "key", "token", "secret", "cookie", "setcookie", "authorization", "proxyauthorization":
		return true
	}
	for _, sensitive := range compactSensitiveNames {
		if compact == sensitive || strings.HasSuffix(compact, sensitive) {
			return true
		}
	}
	return false
}

// RedactHeaders returns a deep copy and never mutates the live request headers.
func RedactHeaders(headers http.Header) http.Header {
	redacted := make(http.Header, len(headers))
	for key, values := range headers {
		if isSensitiveName(key, sensitiveHeaders) {
			redacted[key] = []string{RedactedValue}
			continue
		}
		redacted[key] = append([]string(nil), values...)
	}
	return redacted
}

// RedactQuery returns a deep copy suitable for durable audit artifacts.
func RedactQuery(query url.Values) url.Values {
	redacted := make(url.Values, len(query))
	for key, values := range query {
		if isSensitiveName(key, sensitiveQueryKeys) {
			redacted[key] = []string{RedactedValue}
			continue
		}
		redacted[key] = append([]string(nil), values...)
	}
	return redacted
}

// AuditHeaders mirrors Node's request-header JSON shape: lowercase keys and a
// scalar for a single value. Repeated headers remain arrays.
func AuditHeaders(headers http.Header) map[string]any {
	result := make(map[string]any, len(headers))
	for key, values := range headers {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if isSensitiveName(normalized, sensitiveHeaders) {
			result[normalized] = RedactedValue
			continue
		}
		if len(values) == 1 {
			result[normalized] = values[0]
		} else {
			result[normalized] = append([]string(nil), values...)
		}
	}
	return result
}

// AuditQuery follows the same scalar/array convention used by Express query
// parsing while applying the broader credential-name safety rules.
func AuditQuery(query url.Values) map[string]any {
	result := make(map[string]any, len(query))
	for key, values := range query {
		if isSensitiveName(key, sensitiveQueryKeys) {
			result[key] = RedactedValue
			continue
		}
		if len(values) == 1 {
			result[key] = values[0]
		} else {
			result[key] = append([]string(nil), values...)
		}
	}
	return result
}

func RedactURL(input *url.URL) string {
	if input == nil {
		return ""
	}
	copyURL := *input
	copyURL.RawQuery = RedactQuery(input.Query()).Encode()
	copyURL.User = nil
	return copyURL.String()
}

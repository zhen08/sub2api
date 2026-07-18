package callaudit

import (
	"net/url"
	"path"
	"strings"
)

type RouteClassification struct {
	Eligible bool
	Path     string
	Protocol Protocol
}

var excludedPrefixes = []string{
	"/admin",
	"/admin-next",
	"/api/v1/admin",
	"/api/v1/auth",
	"/api/v1/users",
	"/users",
	"/web",
	"/health",
	"/metrics",
}

var readOnlySuffixes = []string{
	"/models",
	"/usage",
	"/key-info",
	"/me",
	"/body-preview-stats",
	"/body-preview-purge",
}

func NormalizePath(target string) string {
	raw := strings.TrimSpace(target)
	if raw == "" {
		return "/"
	}
	if parsed, err := url.ParseRequestURI(raw); err == nil && parsed.Path != "" {
		raw = parsed.Path
	} else if index := strings.IndexByte(raw, '?'); index >= 0 {
		raw = raw[:index]
	}
	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}
	normalized := path.Clean(raw)
	if normalized == "." {
		normalized = "/"
	}
	return strings.ToLower(normalized)
}

func ClassifyRoute(method, target string) RouteClassification {
	pathname := NormalizePath(target)
	classification := RouteClassification{
		Path:     pathname,
		Protocol: classifyProtocol(pathname),
	}
	if !strings.EqualFold(strings.TrimSpace(method), "POST") {
		return classification
	}
	for _, prefix := range excludedPrefixes {
		if pathname == prefix || strings.HasPrefix(pathname, prefix+"/") {
			return classification
		}
	}
	for _, suffix := range readOnlySuffixes {
		if pathname == suffix || strings.HasSuffix(pathname, suffix) {
			return classification
		}
	}
	if strings.HasSuffix(pathname, "/messages/count_tokens") ||
		strings.HasSuffix(pathname, "/messages/count-tokens") {
		return classification
	}

	classification.Eligible = isAIInvocationPath(pathname)
	return classification
}

func IsEligibleRoute(method, target string) bool {
	return ClassifyRoute(method, target).Eligible
}

func isAIInvocationPath(pathname string) bool {
	if strings.HasSuffix(pathname, "/messages") ||
		strings.HasSuffix(pathname, "/chat/completions") ||
		strings.HasSuffix(pathname, "/completions") ||
		strings.HasSuffix(pathname, "/embeddings") {
		return true
	}
	if pathname == "/responses" || strings.HasSuffix(pathname, "/responses") ||
		strings.Contains(pathname, "/responses/") {
		return true
	}
	return strings.Contains(pathname, ":generatecontent") ||
		strings.Contains(pathname, ":streamgeneratecontent")
}

func classifyProtocol(pathname string) Protocol {
	switch {
	case strings.HasPrefix(pathname, "/azure/"):
		return ProtocolAzureOpenAI
	case strings.HasPrefix(pathname, "/droid/"):
		return ProtocolDroid
	case strings.HasPrefix(pathname, "/antigravity/"):
		return ProtocolAntigravity
	case strings.HasPrefix(pathname, "/gemini-cli/"):
		return ProtocolGeminiCLI
	case strings.HasPrefix(pathname, "/v1beta/") ||
		strings.HasPrefix(pathname, "/gemini/") ||
		strings.HasPrefix(pathname, "/openai/gemini/"):
		return ProtocolGemini
	case strings.HasPrefix(pathname, "/openai/claude/"):
		return ProtocolOpenAIClaude
	case strings.HasPrefix(pathname, "/openai/"):
		return ProtocolOpenAI
	// Preserve claude-relay-service's historical protocol dimension for its
	// mounted compatibility aliases, even when the leaf payload is OpenAI-like.
	case strings.HasPrefix(pathname, "/api/") || strings.HasPrefix(pathname, "/claude/"):
		return ProtocolAnthropic
	case pathname == "/responses" || strings.HasPrefix(pathname, "/responses/") ||
		strings.Contains(pathname, "/chat/completions") ||
		strings.HasSuffix(pathname, "/completions") ||
		strings.HasSuffix(pathname, "/embeddings") ||
		strings.HasSuffix(pathname, "/responses") || strings.Contains(pathname, "/responses/"):
		return ProtocolOpenAI
	case strings.HasSuffix(pathname, "/messages"):
		return ProtocolAnthropic
	default:
		return ProtocolUnknown
	}
}

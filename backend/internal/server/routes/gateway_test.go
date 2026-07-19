package routes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newGatewayRoutesTestRouter(platform ...string) *gin.Engine {
	groupPlatform := service.PlatformOpenAI
	if len(platform) > 0 && platform[0] != "" {
		groupPlatform = platform[0]
	}
	return newGatewayRoutesTestRouterWithAudit(groupPlatform, nil)
}

func newGatewayRoutesTestRouterWithAudit(groupPlatform string, callAudit servermiddleware.CallAuditMiddleware) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	RegisterGatewayRoutes(
		router,
		&handler.Handlers{
			Gateway:       &handler.GatewayHandler{},
			OpenAIGateway: &handler.OpenAIGatewayHandler{},
			AsyncImage:    handler.NewAsyncImageHandler(nil, nil),
		},
		servermiddleware.APIKeyAuthMiddleware(func(c *gin.Context) {
			groupID := int64(1)
			c.Set(string(servermiddleware.ContextKeyAPIKey), &service.APIKey{
				GroupID: &groupID,
				Group:   &service.Group{Platform: groupPlatform},
				User:    &service.User{ID: 1},
			})
			c.Next()
		}),
		callAudit,
		nil,
		nil,
		nil,
		nil,
		&config.Config{},
	)

	return router
}

func TestGatewayRoutesOpenAIResponsesCompactPathIsRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter()

	for _, path := range []string{
		"/v1/responses/compact",
		"/responses/compact",
		"/backend-api/codex/responses",
		"/backend-api/codex/responses/compact",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"gpt-5"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "path=%s should hit OpenAI responses handler", path)
	}
}

func TestGatewayRoutesCRSLegacyCompatibilityAliasesAreRegisteredOnce(t *testing.T) {
	router := newGatewayRoutesTestRouter()
	routeCounts := make(map[string]int)
	for _, route := range router.Routes() {
		routeCounts[route.Method+" "+route.Path]++
	}

	want := []string{
		"POST /api/v1/messages",
		"POST /api/v1/messages/count_tokens",
		"POST /api/v1/chat/completions",
		"POST /api/v1/completions",
		"GET /api/v1/models",
		"POST /claude/v1/messages",
		"POST /claude/v1/messages/count_tokens",
		"POST /claude/v1/completions",
		"GET /claude/v1/models",
		"GET /claude/v1/usage",
		"POST /openai/v1/responses",
		"POST /openai/v1/responses/*subpath",
		"GET /openai/v1/responses",
		"POST /openai/v1/chat/completions",
		"POST /openai/v1/completions",
		"POST /openai/v1/embeddings",
		"GET /openai/v1/models",
		"GET /openai/v1/usage",
		"POST /antigravity/api/v1/messages",
		"POST /antigravity/api/v1/messages/count_tokens",
		"GET /antigravity/api/v1/models",
		"GET /antigravity/api/v1/usage",
		"POST /gemini-cli/api/v1/messages",
		"POST /gemini-cli/api/v1/messages/count_tokens",
		"GET /gemini-cli/api/v1/models",
		"GET /gemini-cli/api/v1/usage",
	}

	for _, route := range want {
		require.Equal(t, 1, routeCounts[route], "%s must be registered exactly once", route)
	}
}

func TestLegacyCompletionsHandlerMapsPromptWithoutChangingResponseContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	var mapped map[string]any
	router.POST("/v1/completions", legacyCompletionsHandler(func(c *gin.Context) {
		require.NoError(t, c.ShouldBindJSON(&mapped))
		c.JSON(http.StatusOK, gin.H{"object": "chat.completion"})
	}))
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"hello","temperature":0.2}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "claude-3-5-sonnet-20241022", mapped["model"])
	require.Equal(t, float64(1), mapped["n"])
	messages, ok := mapped["messages"].([]any)
	require.True(t, ok)
	require.Equal(t, "hello", messages[0].(map[string]any)["content"])

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"gpt"}`))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestEveryGatewayPOSTHasExplicitCallAuditPolicy(t *testing.T) {
	router := newGatewayRoutesTestRouter()
	for _, route := range router.Routes() {
		if route.Method != http.MethodPost {
			continue
		}
		if callaudit.ClassifyRoute(route.Method, route.Path).Eligible {
			continue
		}
		path := strings.ToLower(route.Path)
		intentionallyExcluded := strings.Contains(path, "/count_tokens") ||
			strings.Contains(path, "/images/") || strings.Contains(path, "/videos/") ||
			strings.HasSuffix(path, "/alpha/search")
		// Gemini's action is part of a wildcard segment. The concrete runtime
		// path is classified from :generateContent/:streamGenerateContent.
		dynamicGeminiInvocation := strings.HasSuffix(path, "/models/*modelaction")
		require.True(t, intentionallyExcluded || dynamicGeminiInvocation,
			"POST %s must be explicitly audited or intentionally excluded", route.Path)
	}
	for _, path := range []string{
		"/v1beta/models/gemini-2.5-pro:generateContent",
		"/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
		"/antigravity/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
	} {
		require.True(t, callaudit.ClassifyRoute(http.MethodPost, path).Eligible, path)
	}
	require.False(t, callaudit.ClassifyRoute(http.MethodPost, "/v1beta/models/gemini-2.5-pro:countTokens").Eligible)
}

func TestGatewayRoutesCRSLegacyAliasesRunCallAuditOnceAfterAPIKeyAuth(t *testing.T) {
	auditCalls := 0
	authenticatedAuditCalls := 0
	forcedPlatforms := make([]string, 0, 5)
	audit := servermiddleware.CallAuditMiddleware(func(c *gin.Context) {
		auditCalls++
		if apiKey, ok := servermiddleware.GetAPIKeyFromContext(c); ok && apiKey != nil {
			authenticatedAuditCalls++
		}
		platform, _ := servermiddleware.GetForcePlatformFromContext(c)
		forcedPlatforms = append(forcedPlatforms, platform)
		c.Next()
	})
	router := newGatewayRoutesTestRouterWithAudit(service.PlatformAnthropic, audit)

	for _, path := range []string{
		"/api/v1/messages",
		"/claude/v1/messages",
		"/openai/v1/responses",
		"/antigravity/api/v1/messages",
		"/gemini-cli/api/v1/messages",
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"test","messages":[]}`))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)
	}

	require.Equal(t, 5, auditCalls, "each legacy request must enter call audit exactly once")
	require.Equal(t, auditCalls, authenticatedAuditCalls, "call audit must run after API-key authentication")
	require.Equal(t, []string{"", service.PlatformAnthropic, service.PlatformOpenAI, service.PlatformAntigravity, service.PlatformGemini}, forcedPlatforms)
}

func TestGatewayRoutesOpenAIAlphaSearchPathsAreRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter()
	registered := make(map[string]bool)
	for _, route := range router.Routes() {
		if route.Method == http.MethodPost {
			registered[route.Path] = true
		}
	}

	for _, path := range []string{
		"/v1/alpha/search",
		"/alpha/search",
		"/backend-api/codex/alpha/search",
	} {
		require.True(t, registered[path], "POST %s should be registered", path)
	}
}

func TestGatewayRoutesAlphaSearchRejectsNonOpenAIGroup(t *testing.T) {
	router := newGatewayRoutesTestRouter(service.PlatformGrok)
	req := httptest.NewRequest(http.MethodPost, "/v1/alpha/search", strings.NewReader(`{"model":"gpt-5.6-sol"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "only available for OpenAI groups")
}

func TestGatewayRoutesOpenAIImagesPathsAreRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter()

	for _, path := range []string{
		"/v1/images/generations",
		"/v1/images/edits",
		"/images/generations",
		"/images/edits",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"gpt-image-2","prompt":"draw a cat"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "path=%s should hit OpenAI images handler", path)
	}
}

func TestGatewayRoutesAsyncImagesPathsAreRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter()
	registered := make(map[string]bool)
	for _, route := range router.Routes() {
		registered[route.Method+" "+route.Path] = true
	}

	for _, route := range []string{
		"POST /v1/images/generations/async",
		"POST /v1/images/edits/async",
		"GET /v1/images/tasks/:task_id",
		"POST /images/generations/async",
		"POST /images/edits/async",
		"GET /images/tasks/:task_id",
	} {
		require.True(t, registered[route], "%s should be registered", route)
	}
}

func TestGatewayRoutesGrokImagesAndVideosPathsAreRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter(service.PlatformGrok)

	for _, path := range []string{
		"/v1/images/generations",
		"/v1/images/edits",
		"/images/generations",
		"/images/edits",
		"/v1/videos/generations",
		"/videos/generations",
		"/v1/videos/edits",
		"/videos/edits",
		"/v1/videos/extensions",
		"/videos/extensions",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"grok-imagine","prompt":"draw a cat"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "path=%s should hit Grok media handler", path)
		require.NotContains(t, w.Body.String(), "not supported for this platform")
	}

	for _, path := range []string{
		"/v1/videos/request-123",
		"/videos/request-123",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "path=%s should hit Grok video handler", path)
		require.NotContains(t, w.Body.String(), "not supported for this platform")
	}
}

func TestGatewayRoutesNonGrokVideosAreRejectedAtPlatformGate(t *testing.T) {
	router := newGatewayRoutesTestRouter(service.PlatformOpenAI)

	for _, tc := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/videos/generations", `{"model":"grok-imagine-video-1.5","prompt":"waves"}`},
		{http.MethodPost, "/videos/generations", `{"model":"grok-imagine-video-1.5","prompt":"waves"}`},
		{http.MethodPost, "/v1/videos/edits", `{"model":"grok-imagine-video","prompt":"waves","video":{"url":"https://example.com/in.mp4"}}`},
		{http.MethodPost, "/videos/edits", `{"model":"grok-imagine-video","prompt":"waves","video":{"url":"https://example.com/in.mp4"}}`},
		{http.MethodPost, "/v1/videos/extensions", `{"model":"grok-imagine-video","prompt":"waves","video":{"url":"https://example.com/in.mp4"}}`},
		{http.MethodPost, "/videos/extensions", `{"model":"grok-imagine-video","prompt":"waves","video":{"url":"https://example.com/in.mp4"}}`},
		{http.MethodGet, "/v1/videos/request-123", ""},
		{http.MethodGet, "/videos/request-123", ""},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusNotFound, w.Code, "method=%s path=%s", tc.method, tc.path)
		require.Contains(t, w.Body.String(), "Videos API is not supported for this platform")
	}
}

func TestGatewayRoutesGrokAllowsCLICompatibilityEntrypoints(t *testing.T) {
	router := newGatewayRoutesTestRouter(service.PlatformGrok)

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/messages"},
		{http.MethodPost, "/v1/chat/completions"},
		{http.MethodPost, "/chat/completions"},
		{http.MethodGet, "/v1/responses"},
		{http.MethodGet, "/responses"},
		{http.MethodGet, "/backend-api/codex/responses"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"model":"grok"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "method=%s path=%s", tc.method, tc.path)
		require.NotContains(t, w.Body.String(), "not supported for Grok groups")
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(`{"model":"grok","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "Token counting is not supported for this platform")

	for _, path := range []string{
		"/v1/responses",
		"/responses",
		"/backend-api/codex/responses",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"grok","input":"hi"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		require.NotEqual(t, http.StatusNotFound, w.Code, "path=%s should still reach Responses handler", path)
	}
}

func TestGatewayRoutesOpenAICountTokensPathIsRegistered(t *testing.T) {
	router := newGatewayRoutesTestRouter(service.PlatformOpenAI)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.NotEqual(t, http.StatusNotFound, w.Code)
}

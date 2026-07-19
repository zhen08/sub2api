package middleware

import (
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGatewayPermissionForPath(t *testing.T) {
	tests := map[string]string{
		"/v1/messages":                  service.YunMoStarPermissionClaude,
		"/api/v1/messages/count_tokens": service.YunMoStarPermissionClaude,
		"/claude/v1/messages":           service.YunMoStarPermissionClaude,
		"/v1/responses":                 service.PlatformOpenAI,
		"/v1/chat/completions":          service.PlatformOpenAI,
		"/openai/v1/responses":          service.PlatformOpenAI,
		"/gemini/v1beta/models":         service.PlatformGemini,
		"/v1beta/models":                service.PlatformGemini,
		"/health":                       "",
	}
	for path, expected := range tests {
		t.Run(path, func(t *testing.T) {
			require.Equal(t, expected, gatewayPermissionForPath(path))
		})
	}
}

func TestGatewayPermissionUsesForcedPlatform(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/claude/v1/messages", nil)
	c.Set(string(ContextKeyForcePlatform), service.PlatformAnthropic)
	require.Equal(t, service.YunMoStarPermissionClaude, gatewayPermission(c))

	c.Set(string(ContextKeyForcePlatform), service.PlatformGemini)
	require.Equal(t, service.PlatformGemini, gatewayPermission(c))

	c.Set(string(ContextKeyForcePlatform), service.PlatformAntigravity)
	require.Equal(t, service.PlatformAntigravity, gatewayPermission(c))
}

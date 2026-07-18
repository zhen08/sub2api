package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func TestRequestUsesAPIKeyCredential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		header string
		value  string
		want   bool
	}{
		{name: "jwt", header: "Authorization", value: "Bearer a.b.c", want: false},
		{name: "bearer api key", header: "Authorization", value: "Bearer sk-legacy", want: true},
		{name: "bare api key", header: "Authorization", value: "sk-legacy", want: true},
		{name: "x api key", header: "x-api-key", value: "sk-legacy", want: true},
		{name: "legacy api key", header: "api-key", value: "sk-legacy", want: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
			request.Header.Set(test.header, test.value)
			if got := requestUsesAPIKeyCredential(request); got != test.want {
				t.Fatalf("requestUsesAPIKeyCredential() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDualUsageAuthExecutesExactlyOneAuthChain(t *testing.T) {
	tests := []struct {
		name          string
		authorization string
		wantAPIKey    bool
	}{
		{name: "legacy bearer key", authorization: "Bearer sk-legacy", wantAPIKey: true},
		{name: "management jwt", authorization: "Bearer header.payload.signature", wantAPIKey: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var jwtCalls, apiKeyCalls, jwtGuardCalls, handlerCalls int
			jwtAuth := servermiddleware.JWTAuthMiddleware(func(c *gin.Context) {
				jwtCalls++
				c.Set("jwt-authenticated", true)
				c.Next()
			})
			apiKeyAuth := servermiddleware.APIKeyAuthMiddleware(func(c *gin.Context) {
				apiKeyCalls++
				c.Set(string(servermiddleware.ContextKeyAPIKey), &service.APIKey{ID: 1})
				c.Next()
			})
			router := gin.New()
			router.GET("/api/v1/usage",
				dualUsageAuth(jwtAuth, apiKeyAuth),
				conditionalJWTMiddleware(func(c *gin.Context) {
					jwtGuardCalls++
					c.Next()
				}),
				func(c *gin.Context) {
					handlerCalls++
					_, hasAPIKey := servermiddleware.GetAPIKeyFromContext(c)
					c.JSON(http.StatusOK, gin.H{"api_key": hasAPIKey})
				},
			)

			request := httptest.NewRequest(http.MethodGet, "/api/v1/usage", nil)
			request.Header.Set("Authorization", test.authorization)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK || handlerCalls != 1 {
				t.Fatalf("response=%d handlers=%d body=%s", recorder.Code, handlerCalls, recorder.Body.String())
			}
			if test.wantAPIKey {
				if apiKeyCalls != 1 || jwtCalls != 0 || jwtGuardCalls != 0 {
					t.Fatalf("API-key chain calls: api=%d jwt=%d guard=%d", apiKeyCalls, jwtCalls, jwtGuardCalls)
				}
			} else if jwtCalls != 1 || apiKeyCalls != 0 || jwtGuardCalls != 1 {
				t.Fatalf("JWT chain calls: api=%d jwt=%d guard=%d", apiKeyCalls, jwtCalls, jwtGuardCalls)
			}
		})
	}
}

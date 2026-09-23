package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type userModelPolicyRateLimitRepo struct {
	stubOpenAIAccountRepo
	mu        sync.Mutex
	modelKeys []string
}

func (r *userModelPolicyRateLimitRepo) SetModelRateLimit(_ context.Context, _ int64, modelKey string, _ time.Time, _ ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.modelKeys = append(r.modelKeys, modelKey)
	return nil
}

func (r *userModelPolicyRateLimitRepo) recordedModelKeys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.modelKeys...)
}

func TestOpenAIUserModelPolicyBridgeFinalDispatchMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)
	settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
	_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
	require.NoError(t, err)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_policy\",\"model\":\"gpt-6-luna\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream, settingService: settings}
	account := &Account{ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1}
	// The payload was normalized before the live policy changed to luna.
	payload := []byte(`{"type":"response.create","model":"gpt-6-astra","stream":true,"input":"hi"}`)
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil).WithContext(ctx)
	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "stub", payload, len(payload), "gpt-6-astra", "", "", "", "", 1, func([]byte) error { return nil })
	require.NoError(t, err)
	require.Equal(t, "gpt-6-luna", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, "gpt-6-luna", result.UpstreamModel)
	require.Equal(t, "gpt-6-luna", result.BillingModel)
}

func TestOpenAIUserModelPolicyBridgeFinalDispatchFailureUsesSentModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
	_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
	require.NoError(t, err)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"slow down"}}`)),
	}}
	repo := &userModelPolicyRateLimitRepo{}
	svc := &OpenAIGatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     upstream,
		settingService:   settings,
		rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil),
	}
	account := &Account{
		ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1,
		Credentials: map[string]any{"temp_unschedulable_enabled": true, "temp_unschedulable_rules": []any{map[string]any{
			"error_code": float64(http.StatusTooManyRequests), "keywords": []any{"slow down"}, "duration_minutes": float64(1),
		}}},
	}
	payload := []byte(`{"type":"response.create","model":"gpt-6-astra","stream":true,"input":"hi"}`)
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil).WithContext(ctx)

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "stub", payload, len(payload), "gpt-6-astra", "", "", "", "", 1, func([]byte) error { return nil })
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, "gpt-6-luna", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, []string{"gpt-6-luna"}, repo.recordedModelKeys())
	opsModel, ok := c.Get(OpsUpstreamModelKey)
	require.True(t, ok)
	require.Equal(t, "gpt-6-luna", opsModel)
}

func TestOpenAIUserModelPolicyBridgeFinalDispatchResponseFailedUsesSentModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
	_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
	require.NoError(t, err)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_policy_failed\",\"status\":\"failed\",\"error\":{\"status_code\":503,\"type\":\"server_error\",\"code\":\"server_error\",\"message\":\"custom outage\"}}\n\n",
		)),
	}}
	repo := &userModelPolicyRateLimitRepo{}
	svc := &OpenAIGatewayService{
		cfg:              &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:     upstream,
		settingService:   settings,
		rateLimitService: NewRateLimitService(repo, nil, &config.Config{}, nil, nil),
	}
	account := &Account{
		ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1,
		Credentials: map[string]any{"temp_unschedulable_enabled": true, "temp_unschedulable_rules": []any{map[string]any{
			"error_code": float64(http.StatusServiceUnavailable), "keywords": []any{"custom outage"}, "duration_minutes": float64(1),
		}}},
	}
	payload := []byte(`{"type":"response.create","model":"gpt-6-astra","stream":true,"input":"hi"}`)
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil).WithContext(ctx)

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "stub", payload, len(payload), "gpt-6-astra", "", "", "", "", 1, func([]byte) error { return nil })
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, "gpt-6-luna", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, []string{"gpt-6-luna"}, repo.recordedModelKeys())
	opsModel, ok := c.Get(OpsUpstreamModelKey)
	require.True(t, ok)
	require.Equal(t, "gpt-6-luna", opsModel)
}

func TestOpenAIUserModelPolicyBridgeFinalDispatchPreservesImageBillingModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
	_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
	require.NoError(t, err)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_policy_image\",\"model\":\"gpt-6-luna\",\"status\":\"completed\",\"output\":[{\"id\":\"ig_policy_1\",\"type\":\"image_generation_call\",\"status\":\"completed\",\"result\":\"final-image\"}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
		)),
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}, httpUpstream: upstream, settingService: settings}
	account := &Account{ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1}
	payload := []byte(`{"type":"response.create","model":"gpt-6-astra","stream":true,"input":"draw"}`)
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil).WithContext(ctx)

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(ctx, c, account, "stub", payload, len(payload), "gpt-6-astra", "gpt-image-2", "1K", "", "", 1, func([]byte) error { return nil })
	require.NoError(t, err)
	require.Equal(t, 1, result.ImageCount)
	require.Equal(t, "gpt-6-luna", result.UpstreamModel)
	require.Equal(t, "gpt-image-2", result.BillingModel)
}

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestIndependentPolicyFinalGateMetadata(t *testing.T) {
	for _, tc := range []struct{ request, upstream string }{
		{"openai/GPT-6", "gpt-6-astra"},
		{"gpt-6-sol", "gpt-6-sol"},
		{"gpt-5.6-sol", "gpt-5.6-sol"},
		{"gpt-5.6-terra", "gpt-5.6-terra"},
		{"gpt-5.6-luna", "gpt-5.6-luna"},
	} {
		t.Run(tc.upstream, func(t *testing.T) {
			testIndependentPolicyFinalGateMetadata(t, tc.request, tc.upstream)
		})
	}
}

func testIndependentPolicyFinalGateMetadata(t *testing.T, requestedModel, mappedModel string) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(mode, func(t *testing.T) {
			models := make(chan string, 8)
			results := make(chan *OpenAIForwardResult, 8)
			settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
			completed := func(model string) []byte {
				return []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_policy_%d","status":"completed","model":%q,"output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`, time.Now().UnixNano(), model))
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
					_, _ = settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						return
					}
					defer conn.CloseNow()
					for {
						ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
						_, body, e := conn.Read(ctx)
						cancel()
						if e != nil {
							return
						}
						model := gjson.GetBytes(body, "model").String()
						models <- model
						ctx, cancel = context.WithTimeout(r.Context(), 3*time.Second)
						e = conn.Write(ctx, coderws.MessageText, completed(model))
						cancel()
						if e != nil {
							return
						}
					}
				}
				body, _ := io.ReadAll(r.Body)
				model := gjson.GetBytes(body, "model").String()
				models <- model
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: %s\n\n", completed(model))
			}))
			defer upstream.Close()
			cfg := &config.Config{}
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			cfg.Gateway.OpenAIWS.Enabled = true
			cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			cfg.Gateway.OpenAIWS.OAuthEnabled = true
			cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
			cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
			cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
			// settings defined above
			_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "full")
			require.NoError(t, err)
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			svc := &OpenAIGatewayService{cfg: cfg, settingService: settings, httpUpstream: &policyRealHTTPUpstream{}, cache: &stubGatewayCache{}, openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool}
			account := &Account{ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Credentials: map[string]any{"api_key": "stub", "base_url": upstream.URL, "model_mapping": map[string]any{"gpt-6-sol": mappedModel, "gpt-6-luna": mappedModel, "gpt-5.6-luna": "gpt-5.6-luna", mappedModel: mappedModel}}, Extra: map[string]any{"responses_websockets_v2_enabled": true, "openai_apikey_responses_websockets_v2_mode": mode}}
			ended := make(chan error, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, e := coderws.Accept(w, r, nil)
				if e != nil {
					ended <- e
					return
				}
				defer conn.CloseNow()
				ctx := WithOpenAIUserModelPolicy(r.Context(), 17, nil)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = r.WithContext(ctx)
				readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, first, e := conn.Read(readCtx)
				cancel()
				if e != nil {
					ended <- e
					return
				}
				ended <- svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "stub", first, &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, _ error) { results <- result }})
			}))
			defer gateway.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			for i, level := range []string{"full", "terra", "luna", "full", "luna", "luna", "terra"} {
				_, err = settings.SetUserOpenAIModelPolicy(ctx, 17, level)
				require.NoError(t, err)
				modelField := fmt.Sprintf(`,"model":%q`, requestedModel)
				if i == 2 {
					modelField = ""
				} // omitted subsequent model must not retain old cap
				if i == 4 {
					modelField = `,"model":"gpt-5.6-luna"`
				}
				if i >= 5 {
					modelField = `,"model":"openai/GPT_6_LUNA-high"`
				}
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","input":"hi"`+modelField+`}`)))
				_, event, e := client.Read(ctx)
				require.NoError(t, e, string(event))
				require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String(), string(event))
				want := "gpt-6-luna"
				if i > 0 {
					if level == "full" {
						want = mappedModel
					} else {
						want = map[string]string{"terra": "gpt-6-sol", "luna": "gpt-6-luna"}[level]
					}
				}
				if level == "terra" && requestedModel == "gpt-5.6-luna" {
					want = "gpt-5.6-luna"
				}
				if i == 4 {
					want = "gpt-6-luna"
				}
				// Explicit Luna aliases stay below Terra on the existing connection.
				if i >= 5 {
					want = "gpt-6-luna"
				}
				select {
				case got := <-models:
					require.Equal(t, want, got)
					result := <-results
					require.Equal(t, got, result.UpstreamModel, "final dispatch metadata must match model actually sent")
					if i == 0 {
						require.Equal(t, got, result.BillingModel)
					}
					billingModel := result.BillingModel
					if billingModel == "" {
						billingModel = forwardResultBillingModel(result.Model, result.UpstreamModel)
					}
					if level != "full" || i == 0 || requestedModel == "gpt-6-sol" {
						require.Equal(t, got, billingModel, "effective usage billing model for every restricted turn")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
			select {
			case e := <-ended:
				require.NoError(t, e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestOpenAIUserModelPolicyFinalDispatchFailureUsesSentModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(mode, func(t *testing.T) {
			testOpenAIUserModelPolicyFinalDispatchFailure(t, mode,
				[]byte(`{"type":"error","error":{"status_code":429,"type":"rate_limit_error","code":"rate_limit_exceeded","message":"slow down"}}`),
				http.StatusTooManyRequests, "slow down", true)
		})
	}
}

func TestOpenAIUserModelPolicyFinalDispatchResponseFailedUsesSentModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough} {
		t.Run(mode, func(t *testing.T) {
			testOpenAIUserModelPolicyFinalDispatchFailure(t, mode,
				[]byte(`{"type":"response.failed","response":{"status":"failed","error":{"status_code":503,"type":"server_error","code":"server_error","message":"custom outage"}}}`),
				http.StatusServiceUnavailable, "custom outage", false)
		})
	}
}

func testOpenAIUserModelPolicyFinalDispatchFailure(t *testing.T, mode string, failureEvent []byte, statusCode int, keyword string, expectEarlyExit bool) {
	t.Helper()
	settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
	_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "full")
	require.NoError(t, err)
	sentModels := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, setErr := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
		require.NoError(t, setErr)
		conn, acceptErr := coderws.Accept(w, r, nil)
		if acceptErr != nil {
			return
		}
		defer conn.CloseNow()
		readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		_, body, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			return
		}
		sentModels <- gjson.GetBytes(body, "model").String()
		writeCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		_ = conn.Write(writeCtx, coderws.MessageText, failureEvent)
		cancel()
	}))
	defer upstream.Close()

	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()
	repo := &userModelPolicyRateLimitRepo{}
	svc := &OpenAIGatewayService{
		cfg: cfg, settingService: settings, rateLimitService: NewRateLimitService(repo, nil, cfg, nil, nil),
		httpUpstream: &policyRealHTTPUpstream{}, cache: &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
	}
	account := &Account{
		ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{
			"api_key": "stub", "base_url": upstream.URL,
			"model_mapping":              map[string]any{"gpt-6-astra": "gpt-6-astra", "gpt-6-luna": "gpt-6-luna"},
			"temp_unschedulable_enabled": true,
			"temp_unschedulable_rules": []any{map[string]any{
				"error_code": float64(statusCode), "keywords": []any{keyword}, "duration_minutes": float64(1),
			}},
		},
		Extra: map[string]any{"responses_websockets_v2_enabled": true, "openai_apikey_responses_websockets_v2_mode": mode},
	}
	type proxyOutcome struct {
		err error
		c   *gin.Context
	}
	ended := make(chan proxyOutcome, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, acceptErr := coderws.Accept(w, r, nil)
		if acceptErr != nil {
			ended <- proxyOutcome{err: acceptErr}
			return
		}
		defer conn.CloseNow()
		ctx := WithOpenAIUserModelPolicy(r.Context(), 17, nil)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r.WithContext(ctx)
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, first, readErr := conn.Read(readCtx)
		cancel()
		if readErr != nil {
			ended <- proxyOutcome{err: readErr, c: c}
			return
		}
		ended <- proxyOutcome{err: svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "stub", first, nil), c: c}
	}))
	defer gateway.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"openai/GPT-6","input":"hi"}`)))

	select {
	case got := <-sentModels:
		require.Equal(t, "gpt-6-luna", got)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !expectEarlyExit {
		_, event, readErr := client.Read(ctx)
		require.NoError(t, readErr)
		require.Equal(t, "response.failed", gjson.GetBytes(event, "type").String())
		require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
	}
	select {
	case outcome := <-ended:
		if expectEarlyExit {
			require.Error(t, outcome.err)
		}
		keys := repo.recordedModelKeys()
		require.NotEmpty(t, keys)
		for _, key := range keys {
			require.Equal(t, "gpt-6-luna", key, "failure-side account state must use the model actually dispatched")
		}
		opsModel, ok := outcome.c.Get(OpsUpstreamModelKey)
		require.True(t, ok)
		require.Equal(t, "gpt-6-luna", opsModel)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

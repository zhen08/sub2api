//go:build unit

package handler

import (
	"bytes"
	"context"
	coderws "github.com/coder/websocket"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type userPolicyHandlerSettings struct {
	service.SettingRepository
	mu     sync.Mutex
	values map[string]string
}

func (r *userPolicyHandlerSettings) GetValue(_ context.Context, k string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.values[k]
	if !ok {
		return "", service.ErrSettingNotFound
	}
	return v, nil
}
func (r *userPolicyHandlerSettings) Set(_ context.Context, k, v string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[k] = v
	return nil
}

type userPolicyHandlerUpstream struct {
	service.HTTPUpstream
	mu     sync.Mutex
	models []string
}

func (u *userPolicyHandlerUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	b, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	model := gjson.GetBytes(b, "model").String()
	u.mu.Lock()
	u.models = append(u.models, model)
	u.mu.Unlock()
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(bytes.NewBufferString("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_policy\",\"status\":\"completed\",\"model\":\"" + model + "\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"))}, nil
}
func TestOpenAIUserModelPolicyHTTPIngress(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"responses", "chat/completions", "messages"} {
		t.Run(path, func(t *testing.T) {
			repo := openAIImagesFailoverAccountRepo{accounts: []service.Account{{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"access_token": "stub-token", "model_mapping": map[string]any{"gpt-6-sol": "gpt-6-astra", "gpt-6-luna": "gpt-6-astra"}}}}}
			cfg := &config.Config{RunMode: config.RunModeSimple}
			upstream := &userPolicyHandlerUpstream{}
			settings := service.NewSettingService(&userPolicyHandlerSettings{values: map[string]string{}}, cfg)
			_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "terra")
			require.NoError(t, err)
			gateway := service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, settings, nil)
			billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			defer billing.Stop()
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			gid := int64(6)
			key := &service.APIKey{ID: 88, UserID: 17, User: &service.User{ID: 17}, GroupID: &gid, Group: &service.Group{ID: gid, Platform: service.PlatformOpenAI, AllowMessagesDispatch: true}}
			for _, level := range []string{"terra", "luna"} {
				_, err = settings.SetUserOpenAIModelPolicy(context.Background(), 17, level)
				require.NoError(t, err)
				body := `{"model":"openai/GPT-6","stream":false,"input":"hi","messages":[{"role":"user","content":"hi"}],"max_tokens":32}`
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest("POST", "/v1/"+path, bytes.NewBufferString(body))
				c.Request.Header.Set("Content-Type", "application/json")
				c.Request = c.Request.WithContext(service.WithOpenAIUserModelPolicy(c.Request.Context(), 17, &gid))
				c.Set(string(middleware.ContextKeyAPIKey), key)
				c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 17})
				switch path {
				case "responses":
					h.Responses(c)
				case "messages":
					h.Messages(c)
				default:
					h.ChatCompletions(c)
				}
				require.Equal(t, 200, rec.Code, rec.Body.String())
				require.NotEmpty(t, upstream.models, rec.Body.String())
				require.Equal(t, map[string]string{"terra": "gpt-6-sol", "luna": "gpt-6-luna"}[level], upstream.models[len(upstream.models)-1])
				actualModel, _ := c.Get(service.OpsUpstreamModelKey)
				require.Equal(t, map[string]string{"terra": "gpt-6-sol", "luna": "gpt-6-luna"}[level], actualModel, "audit metadata must match actual dispatch")
			}
		})
	}
}

func TestOpenAIUserModelPolicyWSTurnChargeSelection(t *testing.T) {
	for _, source := range []string{service.BillingModelSourceRequested, service.BillingModelSourceChannelMapped, service.BillingModelSourceUpstream} {
		for _, model := range []string{"gpt-6-sol", "gpt-6-luna", "gpt-5.6-luna"} {
			t.Run(source+"/"+model, func(t *testing.T) {
				result := &service.OpenAIForwardResult{BillingModel: model, UserPolicyBillingModel: model}
				mapping := service.ChannelMappingResult{MappedModel: "gpt-5.6-sol", BillingModelSource: source}
				require.Equal(t, model, openAIWSTurnBillingModel(result, mapping, "gpt-6-astra", "gpt-6-astra"))
			})
		}
	}
	// Dedicated image billing must not inherit a text-policy marker.
	result := &service.OpenAIForwardResult{BillingModel: "gpt-image-2", UserPolicyBillingModel: "gpt-6-sol", ImageCount: 1}
	require.Equal(t, "gpt-image-2", openAIWSTurnBillingModel(result, service.ChannelMappingResult{}, "gpt-6-astra", "gpt-6-astra"))
}

type userPolicyChannelRepo struct{ service.ChannelRepository }

func (*userPolicyChannelRepo) ListAll(context.Context) ([]service.Channel, error) {
	return []service.Channel{{ID: 1, Status: service.StatusActive, GroupIDs: []int64{6, 8}, ModelMapping: map[string]map[string]string{service.PlatformOpenAI: {"gpt-5.6-sol": "gpt-5.6-terra", "codex-auto-review": "gpt-5.6-luna"}}}}, nil
}
func (*userPolicyChannelRepo) GetGroupPlatforms(context.Context, []int64) (map[int64]string, error) {
	return map[int64]string{6: service.PlatformOpenAI, 8: service.PlatformOpenAI}, nil
}

func TestOpenAIUserModelPolicyIsolationAndRecovery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := openAIImagesFailoverAccountRepo{accounts: []service.Account{{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"access_token": "stub"}}}}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	upstream := &userPolicyHandlerUpstream{}
	settings := service.NewSettingService(&userPolicyHandlerSettings{values: map[string]string{}}, cfg)
	channels := service.NewChannelService(&userPolicyChannelRepo{}, nil, nil, nil, nil)
	gateway := service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, channels, nil, settings, nil)
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	defer billing.Stop()
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	for _, tc := range []struct {
		user, key, group   int64
		level, model, want string
	}{
		{17, 1, 6, "luna", "gpt-6-astra", "gpt-6-luna"},
		{17, 999, 8, "luna", "gpt-6-astra", "gpt-6-luna"}, // new key/self group switch
		{18, 2, 6, "original", "gpt-5.6-sol", "gpt-5.6-terra"},
		{17, 1, 6, "full", "gpt-5.6-sol", "gpt-5.6-sol"},
		{17, 1, 6, "full", "codex-auto-review", "codex-auto-review"},
		{18, 2, 6, "original", "codex-auto-review", "gpt-5.6-luna"},
	} {
		if tc.level != "original" {
			_, err := settings.SetUserOpenAIModelPolicy(context.Background(), tc.user, tc.level)
			require.NoError(t, err)
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/responses", bytes.NewBufferString(`{"model":"`+tc.model+`","input":"hi","stream":false}`))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Request = c.Request.WithContext(service.WithOpenAIUserModelPolicy(c.Request.Context(), tc.user, &tc.group))
		c.Set(string(middleware.ContextKeyAPIKey), &service.APIKey{ID: tc.key, UserID: tc.user, User: &service.User{ID: tc.user}, GroupID: &tc.group, Group: &service.Group{ID: tc.group, Platform: service.PlatformOpenAI}})
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: tc.user})
		h.Responses(c)
		require.Equal(t, 200, c.Writer.Status(), tc)
		require.Equal(t, tc.want, upstream.models[len(upstream.models)-1], tc)
	}
}

func TestOpenAIUserModelPolicyWebSocketHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	repo := &openAIWSFailoverHandlerAccountRepoStub{accounts: []service.Account{{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "stub", "base_url": "https://stub.invalid"}, Extra: map[string]any{"openai_apikey_responses_websockets_v2_enabled": true, "openai_apikey_responses_websockets_v2_mode": service.OpenAIWSIngressModeHTTPBridge}}}}
	upstream := &userPolicyHandlerUpstream{}
	settings := service.NewSettingService(&userPolicyHandlerSettings{values: map[string]string{}}, cfg)
	channels := service.NewChannelService(&userPolicyChannelRepo{}, nil, nil, nil, nil)
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	defer billing.Stop()
	gateway := service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, cfg, nil, nil, service.NewBillingService(cfg, nil), nil, billing, upstream, &service.DeferredService{}, nil, nil, nil, channels, nil, settings, nil)
	cache := &concurrencyCacheMock{acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil }, acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil }}
	h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(cache), billing, &service.APIKeyService{}, nil, nil, nil, nil, cfg)
	gid := int64(6)
	key := &service.APIKey{ID: 88, UserID: 17, User: &service.User{ID: 17, Status: service.StatusActive}, GroupID: &gid, Group: &service.Group{ID: gid, Platform: service.PlatformOpenAI, Status: service.StatusActive}}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 17, Concurrency: 1})
		c.Request = c.Request.WithContext(service.WithOpenAIUserModelPolicy(c.Request.Context(), 17, &gid))
		c.Next()
	})
	done := make(chan struct{})
	router.GET("/v1/responses", func(c *gin.Context) { defer close(done); h.ResponsesWebSocket(c) })
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	for i, level := range []string{"full", "terra", "luna", "full"} {
		_, err = settings.SetUserOpenAIModelPolicy(ctx, 17, level)
		require.NoError(t, err)
		field := `,"model":"gpt-5.6-sol"`
		if i == 2 {
			field = ""
		}
		require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","input":"hi"`+field+`}`)))
		_, body, e := conn.Read(ctx)
		require.NoError(t, e, string(body))
		require.Equal(t, "response.completed", gjson.GetBytes(body, "type").String(), string(body))
		want := "gpt-5.6-sol"
		if level != "full" {
			want = map[string]string{"terra": "gpt-6-sol", "luna": "gpt-6-luna"}[level]
		}
		upstream.mu.Lock()
		actual := upstream.models[len(upstream.models)-1]
		upstream.mu.Unlock()
		require.Equal(t, want, actual)
	}
	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

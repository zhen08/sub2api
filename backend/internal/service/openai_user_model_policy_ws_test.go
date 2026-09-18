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

type policyRealHTTPUpstream struct{ HTTPUpstream }

func (*policyRealHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}

func TestOpenAIUserModelPolicyExistingWebSocket(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough, OpenAIWSIngressModeHTTPBridge} {
		t.Run(mode, func(t *testing.T) {
			models := make(chan string, 8)
			completed := func(model string) []byte {
				return []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_policy_%d","status":"completed","model":%q,"output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`, time.Now().UnixNano(), model))
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
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
			settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, cfg)
			_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "full")
			require.NoError(t, err)
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			svc := &OpenAIGatewayService{cfg: cfg, settingService: settings, httpUpstream: &policyRealHTTPUpstream{}, cache: &stubGatewayCache{}, openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool}
			account := &Account{ID: 901, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Credentials: map[string]any{"api_key": "stub", "base_url": upstream.URL, "model_mapping": map[string]any{"gpt-5.6-terra": "gpt-6-astra", "gpt-5.6-luna": "gpt-5.6-luna", "gpt-6-astra": "gpt-6-astra"}}, Extra: map[string]any{"responses_websockets_v2_enabled": true, "openai_apikey_responses_websockets_v2_mode": mode}}
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
				ended <- svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "stub", first, nil)
			}))
			defer gateway.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(gateway.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			for i, level := range []string{"full", "terra", "luna", "full"} {
				_, err = settings.SetUserOpenAIModelPolicy(ctx, 17, level)
				require.NoError(t, err)
				modelField := `,"model":"openai/GPT-6"`
				if i == 2 {
					modelField = ""
				} // omitted subsequent model must not retain old cap
				require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","input":"hi"`+modelField+`}`)))
				_, event, e := client.Read(ctx)
				require.NoError(t, e, string(event))
				require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String(), string(event))
				want := "gpt-6-astra"
				if level != "full" {
					want = "gpt-5.6-" + level
				}
				select {
				case got := <-models:
					require.Equal(t, want, got)
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

func TestOpenAIUserModelPolicyPooledConnectionUserIsolation(t *testing.T) {
	repo := &userModelPolicyRepo{values: map[string]string{}}
	settings := NewSettingService(repo, nil)
	_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
	require.NoError(t, err)
	s := &OpenAIGatewayService{settingService: settings}
	physical := &openAIWSCaptureConn{}
	conn := &openAIWSConn{ws: physical}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	for _, id := range []int64{17, 18, 17} {
		lease := &openAIWSConnLease{conn: conn}
		ctx := WithOpenAIUserModelPolicy(context.Background(), id, nil)
		require.NoError(t, s.writeUserPolicyWSJSON(ctx, lease, account, map[string]any{"type": "response.create", "model": "gpt-6-astra"}))
	}
	require.Len(t, physical.writes, 3)
	require.Equal(t, "gpt-5.6-luna", physical.writes[0]["model"])
	require.Equal(t, "gpt-6-astra", physical.writes[1]["model"])
	require.Equal(t, "gpt-5.6-luna", physical.writes[2]["model"])
	repo.mu.Lock()
	repo.err = fmt.Errorf("offline")
	repo.mu.Unlock()
	err = s.writeUserPolicyWSJSON(WithOpenAIUserModelPolicy(context.Background(), 17, nil), &openAIWSConnLease{conn: conn}, account, map[string]any{"type": "response.create", "model": "gpt-6-astra"})
	require.Error(t, err)
	require.Len(t, physical.writes, 3)
}

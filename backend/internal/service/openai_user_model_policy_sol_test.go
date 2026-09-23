package service

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIUserModelPolicySolFinalHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, path := range []string{"responses", "responses/compact", "chat/completions", "messages"} {
		t.Run(path, func(t *testing.T) {
			settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
			channel := &ChannelService{}
			channel.cache.Store(populateChannelCache([]Channel{{ID: 1, Status: StatusActive, GroupIDs: []int64{6}, ModelMapping: map[string]map[string]string{PlatformOpenAI: {
				"public-sol": "gpt-6-sol", "gpt-6-sol": "gpt-6-astra", "gpt-6-luna": "gpt-6-sol",
			}}}}, map[int64]string{6: PlatformOpenAI}))
			upstream := &policyCaptureUpstream{}
			svc := &OpenAIGatewayService{settingService: settings, channelService: channel, httpUpstream: upstream}
			group := int64(6)
			identity := WithOpenAIUserModelPolicy(context.Background(), 10, &group)
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{
				"gpt-6-sol": "gpt-6-sol", "gpt-6-luna": "gpt-6-sol",
			}}}
			// The same authenticated identity sees each durable policy change.
			for _, level := range []string{"original", "terra", "luna", "full"} {
				t.Run(level, func(t *testing.T) {
					if level != "original" {
						_, err := settings.SetUserOpenAIModelPolicy(identity, 10, level)
						require.NoError(t, err)
					}
					want := "gpt-6-sol"
					if level == "luna" {
						want = "gpt-6-luna"
					}
					mapped := "gpt-6-sol"
					if level == "terra" || level == "luna" {
						for _, request := range []string{"gpt-6-sol", "public-sol", "gpt-6-astra"} {
							mapping, err := svc.ResolveUserOpenAIChannelMapping(identity, &group, request)
							require.NoError(t, err)
							require.Equal(t, want, mapping.MappedModel, request)
							mapped = account.GetMappedModel(normalizeCodexModel(mapping.MappedModel))
							require.Equal(t, "gpt-6-sol", mapped)
						}
					}
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					ctx, finish := beginUserModelDispatch(identity, c)
					req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://stub/v1/"+path, bytes.NewBufferString(`{"model":"`+mapped+`"}`))
					require.NoError(t, err)
					response, err := svc.doOpenAIUpstream(req, "", account)
					require.NoError(t, err)
					require.NoError(t, response.Body.Close())
					require.Equal(t, want, upstream.models[len(upstream.models)-1])
					result := &OpenAIForwardResult{Model: "gpt-6-sol", UpstreamModel: mapped, BillingModel: mapped}
					finish(result)
					require.Equal(t, "gpt-6-sol", result.Model)
					require.Equal(t, want, result.UpstreamModel)
					require.Equal(t, want, result.BillingModel)
					if level != "original" {
						opsModel, ok := c.Get(OpsUpstreamModelKey)
						require.True(t, ok)
						require.Equal(t, want, opsModel)
					}
				})
			}
		})
	}
}

func TestOpenAIUserModelPolicySolRank(t *testing.T) {
	for _, tc := range []struct{ model, terra, luna string }{
		{"gpt-6-astra", "gpt-6-sol", "gpt-6-luna"},
		{"gpt-5.6-sol", "gpt-6-sol", "gpt-6-luna"},
		{"gpt-6-sol", "gpt-6-sol", "gpt-6-luna"},
		{"gpt-5.6-terra", "gpt-6-sol", "gpt-6-luna"},
		{"gpt-5.6-luna", "gpt-5.6-luna", "gpt-6-luna"},
		{"gpt-6-luna", "gpt-6-luna", "gpt-6-luna"},
	} {
		t.Run(tc.model, func(t *testing.T) {
			t.Run("terra", func(t *testing.T) {
				require.Equal(t, tc.terra, clampUserOpenAIModel("terra", tc.model))
			})
			t.Run("luna", func(t *testing.T) {
				require.Equal(t, tc.luna, clampUserOpenAIModel("luna", tc.model))
			})
			require.Equal(t, tc.model, clampUserOpenAIModel("full", tc.model))
			require.Equal(t, tc.model, clampUserOpenAIModel("original", tc.model))
		})
	}
}

func TestOpenAIUserModelPolicySolAliases(t *testing.T) {
	for _, model := range []string{"gpt-6-sol", "openai/GPT_6_SOL", "gpt-6-sol-high", "gpt-6-sol-2026-09-23", "gpt-6-sol-openai-compact"} {
		t.Run(model, func(t *testing.T) {
			require.Equal(t, "gpt-6-sol", normalizeKnownOpenAICodexModel(model))
			require.Equal(t, "gpt-6-sol", normalizeCodexModel(model))
			require.False(t, isOpenAIGPT6AstraModel(model))
			require.Equal(t, "gpt-6-sol", clampUserOpenAIModel("terra", model))
			require.Equal(t, "gpt-6-luna", clampUserOpenAIModel("luna", model))
			require.Equal(t, "gpt-6-sol", clampUserOpenAIModel("full", model))
			require.Equal(t, model, clampUserOpenAIModel("original", model))
			candidates := usageBillingModelCandidates(model)
			require.Contains(t, candidates, "gpt-6-sol")
			require.NotContains(t, candidates, "gpt-5.6-sol")
			require.NotContains(t, candidates, "gpt-6-astra")
		})
	}
	for _, model := range []string{"gpt-6-solar", "gpt-6-sol-custom", "custom-gpt-6-sol", "gpt-6-solstice", "gpt-6-sol-20260923", "gpt-6-other", "claude-sonnet-4", "gpt-4o"} {
		t.Run("unknown/"+model, func(t *testing.T) {
			require.Empty(t, normalizeKnownOpenAICodexModel(model))
			require.Equal(t, model, normalizeCodexModel(model))
			for _, level := range []string{"terra", "luna", "full", "original"} {
				require.Equal(t, model, clampUserOpenAIModel(level, model))
			}
		})
	}
}

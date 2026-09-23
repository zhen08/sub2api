package service

import (
	"bytes"
	"context"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Exercise the common last HTTP boundary used by all inference protocols after
// ingress channel selection, normalization and an adversarial account mapping.
func TestOpenAIUserModelPolicyLunaFinalRemaps(t *testing.T) {
	settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
	ch := &ChannelService{}
	ch.cache.Store(populateChannelCache([]Channel{{ID: 1, Status: StatusActive, GroupIDs: []int64{6}, ModelMapping: map[string]map[string]string{PlatformOpenAI: {"gpt-6-luna": "gpt-6-astra"}}}}, map[int64]string{6: PlatformOpenAI}))
	upstream := &policyCaptureUpstream{}
	svc := &OpenAIGatewayService{settingService: settings, channelService: ch, httpUpstream: upstream}
	group := int64(6)
	identity := WithOpenAIUserModelPolicy(context.Background(), 17, &group)
	_, err := settings.SetUserOpenAIModelPolicy(identity, 17, "luna")
	require.NoError(t, err)
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"gpt-6-luna": "gpt-6-astra"}}}
	for _, path := range []string{"responses", "responses/compact", "chat/completions", "messages"} {
		t.Run(path, func(t *testing.T) {
			mapping, err := svc.ResolveUserOpenAIChannelMapping(identity, &group, "gpt-6-astra")
			require.NoError(t, err)
			require.Equal(t, "gpt-6-luna", mapping.MappedModel)
			normalized := normalizeCodexModel(mapping.MappedModel)
			require.Equal(t, "gpt-6-luna", normalized)
			mapped := account.GetMappedModel(normalized)
			require.Equal(t, "gpt-6-astra", mapped)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx, finish := beginUserModelDispatch(identity, c)
			req, err := http.NewRequestWithContext(ctx, "POST", "https://stub/v1/"+path, bytes.NewBufferString(`{"model":"`+mapped+`"}`))
			require.NoError(t, err)
			_, err = svc.doOpenAIUpstream(req, "", account)
			require.NoError(t, err)
			require.Equal(t, "gpt-6-luna", upstream.models[len(upstream.models)-1])
			result := &OpenAIForwardResult{Model: "gpt-6-astra", BillingModel: mapped}
			finish(result)
			require.Equal(t, "gpt-6-luna", result.UpstreamModel)
			require.Equal(t, "gpt-6-luna", result.BillingModel)
			opsModel, ok := c.Get(OpsUpstreamModelKey)
			require.True(t, ok)
			require.Equal(t, "gpt-6-luna", opsModel)
		})
	}
}

func TestOpenAIUserModelPolicyAlreadyClampedBilling(t *testing.T) {
	for _, tc := range []struct {
		level, model, billing string
		images                int
	}{
		{"luna", "gpt-6-luna", "gpt-6-luna", 0},
		{"luna", "gpt-5.6-luna", "gpt-6-luna", 0},
		{"terra", "gpt-6-luna", "gpt-6-luna", 0},
		{"terra", "gpt-6-sol", "gpt-6-sol", 0},
		{"terra", "gpt-5.6-sol", "gpt-6-sol", 0},
		{"terra", "gpt-5.6-terra", "gpt-6-sol", 0},
		{"terra", "gpt-5.6-luna", "gpt-5.6-luna", 0},
		{"luna", "gpt-5.6-sol", "gpt-6-luna", 0},
		{"luna", "gpt-5.6-terra", "gpt-6-luna", 0},
		{"luna", "gpt-6-luna", "gpt-image-2", 1},
		{"luna", "claude-sonnet-4", "custom-billing", 0},
		{"full", "gpt-6-luna", "custom-billing", 0},
		{"original", "gpt-6-luna", "custom-billing", 0},
	} {
		t.Run(tc.level+"/"+tc.model+"/"+tc.billing, func(t *testing.T) {
			settings := NewSettingService(&userModelPolicyRepo{values: map[string]string{}}, nil)
			identity := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
			if tc.level != "original" {
				_, err := settings.SetUserOpenAIModelPolicy(identity, 17, tc.level)
				require.NoError(t, err)
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx, finish := beginUserModelDispatch(identity, c)
			svc := &OpenAIGatewayService{settingService: settings}
			_, err := svc.finalizeUserOpenAIModelBody(ctx, nil, []byte(`{"model":"`+tc.model+`"}`))
			require.NoError(t, err)
			initialBilling := "custom-billing"
			if tc.images > 0 {
				initialBilling = "gpt-image-2"
			}
			result := &OpenAIForwardResult{Model: "gpt-6-astra", BillingModel: initialBilling, ImageCount: tc.images}
			finish(result)
			require.Equal(t, tc.billing, result.BillingModel)
		})
	}
}

func TestOpenAIUserModelPolicyLunaAliases(t *testing.T) {
	for _, model := range []string{"gpt-6-luna", "openai/GPT_6_LUNA", "gpt-6-luna-high", "gpt-6-luna-2026-09-23", "gpt-6-luna-openai-compact"} {
		t.Run(model, func(t *testing.T) {
			require.Equal(t, "gpt-6-luna", normalizeCodexModel(model))
			require.Equal(t, "gpt-6-luna", normalizeKnownOpenAICodexModel(model))
			require.False(t, isOpenAIGPT6AstraModel(model))
			for _, level := range []string{"terra", "luna", "full"} {
				require.Equal(t, "gpt-6-luna", clampUserOpenAIModel(level, model), level)
			}
			require.Equal(t, model, clampUserOpenAIModel("original", model))
		})
	}
	for _, level := range []string{"terra", "luna", "full"} {
		require.Equal(t, map[string]string{"terra": "gpt-5.6-luna", "luna": "gpt-6-luna", "full": "gpt-5.6-luna"}[level], clampUserOpenAIModel(level, "openai/GPT-5.6-LUNA-high"))
		for _, model := range []string{"claude-sonnet-4", "gemini-2.5-pro", "gpt-4o", "gpt-6-lunatic"} {
			require.Equal(t, model, clampUserOpenAIModel(level, model))
		}
	}
}

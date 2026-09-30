package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The strict policy path must retain upstream's generic effort maps and group
// per-request tiers while forbidding unrelated catalog price substitution.
func TestOpenAIUserModelPolicyMergeConfiguredPricing(t *testing.T) {
	for _, mode := range []BillingMode{BillingModeToken, BillingModePerRequest} {
		t.Run(string(mode), func(t *testing.T) {
			svc := newOpenAIRecordUsageServiceForTest(&openAIRecordUsageLogRepoStub{}, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{}, nil)
			svc.billingService = NewBillingService(svc.cfg, nil)
			svc.resolver = NewModelPricingResolver(nil, svc.billingService)
			price := 2e-6
			requestPrice := 0.25
			card := ChannelModelPricing{Models: []string{"gpt-6.1-sol"}, BillingMode: mode, InputPrice: &price, OutputPrice: &price, ReasoningEffortMultipliers: map[string]float64{"high": 3}}
			if mode == BillingModePerRequest {
				card.Intervals = []PricingInterval{{MinTokens: 0, PerRequestPrice: &requestPrice}}
			}
			key := &APIKey{Group: &Group{ID: 5, ModelPricing: []ChannelModelPricing{card}}}
			effort := "high"
			result := &OpenAIForwardResult{UserPolicyBillingModel: "gpt-6.1-sol", ReasoningEffort: &effort}
			cost, err := svc.calculatePolicyTextUsageCost(context.Background(), result, key, []string{"gpt-6.1-sol"}, UsageTokens{InputTokens: 20, OutputTokens: 10}, 1.1, "", nil, time.Now())
			require.NoError(t, err)
			expected := 30 * price * 3 * 1.1
			if mode == BillingModePerRequest {
				expected = requestPrice * 3 * 1.1
			}
			require.InDelta(t, expected, cost.ActualCost, 1e-12)
		})
	}
}

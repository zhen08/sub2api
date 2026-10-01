package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Feed real HTTP/WS final-dispatch results into the actual deduction path, with
// adversarial billing-source overrides and other-tier prices still available.
func assertSol61DispatchCharge(t *testing.T, dispatched *OpenAIForwardResult) {
	t.Helper()
	require.Equal(t, "gpt-6.1-sol", dispatched.UpstreamModel)
	require.Equal(t, "gpt-6.1-sol", dispatched.UserPolicyBillingModel)
	for _, pricingSource := range []string{"catalog", "native", "unavailable"} {
		for _, unified := range []bool{false, true} {
			for _, source := range []string{BillingModelSourceRequested, BillingModelSourceChannelMapped, BillingModelSourceUpstream, BillingModelSourceResponse} {
				usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
				userRepo := &openAIRecordUsageUserRepoStub{}
				svc := newOpenAIRecordUsageServiceForTest(usageRepo, userRepo, &openAIRecordUsageSubRepoStub{}, nil)
				prices := map[string]*LiteLLMModelPricing{}
				for _, other := range []string{"gpt-6-sol", "gpt-5.6-sol", "gpt-6-astra", "gpt-6-luna", "gpt-6", "gpt-6.1", "gpt-5.4"} {
					prices[other] = &LiteLLMModelPricing{InputCostPerToken: 9e-6, OutputCostPerToken: 9e-6}
				}
				if pricingSource == "catalog" {
					prices["gpt-6.1-sol"] = &LiteLLMModelPricing{InputCostPerToken: 2e-6, OutputCostPerToken: 2e-6}
				}
				svc.billingService = NewBillingService(svc.cfg, &PricingService{pricingData: prices})
				if pricingSource == "unavailable" {
					// v0.2.11 supplies an exact built-in Sol 6.1 price. Remove it
					// only when exercising genuinely unavailable pricing.
					delete(svc.billingService.fallbackPrices, "gpt-6.1-sol")
				}
				key := &APIKey{ID: 10}
				if unified {
					svc.resolver = NewModelPricingResolver(nil, svc.billingService)
					key.Group = &Group{ID: 5}
				}
				result := *dispatched
				result.Model = "gpt-6-astra"
				result.UpstreamResponseModel = "gpt-6-sol"
				result.Usage = OpenAIUsage{InputTokens: 20, OutputTokens: 10}
				err := svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{Result: &result, APIKey: key, User: &User{ID: 17}, Account: &Account{ID: 30}, ChannelUsageFields: ChannelUsageFields{OriginalModel: "gpt-6-astra", ChannelMappedModel: "gpt-6-sol", BillingModelSource: source}})
				require.NoError(t, err)
				require.NotNil(t, usageRepo.lastLog)
				expected := 0.0
				switch pricingSource {
				case "catalog":
					expected = 30 * 2e-6 * 1.1
				case "native":
					expected = (20*2e-6 + 10*10e-6) * 1.1
				case "unavailable":
					require.Zero(t, userRepo.deductCalls)
				}
				require.InDelta(t, expected, usageRepo.lastLog.ActualCost, 1e-12, pricingSource)
				require.InDelta(t, expected, userRepo.lastAmount, 1e-12, pricingSource)
			}
		}
	}
}

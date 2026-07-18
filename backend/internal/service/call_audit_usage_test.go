package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
)

func TestMergeCallAuditUsageMapsActualAndBaseCost(t *testing.T) {
	t.Parallel()
	spool, err := callaudit.NewSpool(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := callaudit.NewScope(callaudit.ScopeInput{
		RequestID: "usage-cost-map",
		Endpoint:  "/v1/messages",
		Method:    "POST",
	}, 180, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	session, err := callaudit.NewSession(scope, spool, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := callaudit.WithSession(context.Background(), session)
	mergeCallAuditUsage(ctx, &Account{
		ID:   42,
		Type: AccountTypeOAuth,
		Extra: map[string]any{
			"crs_account_id": "legacy-account-42",
			"crs_kind":       "openai-responses",
		},
	}, "claude-final", UsageTokens{
		InputTokens:         10,
		ImageInputTokens:    2,
		OutputTokens:        4,
		ImageOutputTokens:   1,
		CacheReadTokens:     3,
		CacheCreationTokens: 5,
	}, &CostBreakdown{ActualCost: 0.0123, TotalCost: 0.01})

	usage := session.UsageSnapshot()
	if usage.AccountID != "legacy-account-42" || usage.AccountType != "openai-responses" || usage.Model != "claude-final" ||
		usage.InputTokens != 10 || usage.OutputTokens != 4 || usage.CacheReadTokens != 3 || usage.CacheCreateTokens != 5 ||
		usage.TotalTokens != 22 || usage.Cost != "0.0123000000" || usage.RealCost != "0.0100000000" {
		t.Fatalf("usage mapping = %+v", usage)
	}
}

func TestCallAuditAccountSnapshotFallsBackToNativeIdentity(t *testing.T) {
	t.Parallel()
	id, kind := callAuditAccountSnapshot(&Account{ID: 7, Type: AccountTypeAPIKey})
	if id != "7" || kind != AccountTypeAPIKey {
		t.Fatalf("native account snapshot = %q/%q", id, kind)
	}
}

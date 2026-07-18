package service

import (
	"context"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
)

func mergeCallAuditUsage(ctx context.Context, account *Account, model string, tokens UsageTokens, cost *CostBreakdown) {
	session, ok := callaudit.SessionFromContext(ctx)
	if !ok || session == nil {
		return
	}
	usage := callaudit.Usage{
		Model: model,
		// InputTokens/OutputTokens already include their image-token subsets.
		// Adding the detail counters again would double-count multimodal usage.
		InputTokens:       int64(tokens.InputTokens),
		OutputTokens:      int64(tokens.OutputTokens),
		CacheReadTokens:   int64(tokens.CacheReadTokens),
		CacheCreateTokens: int64(tokens.CacheCreationTokens),
	}
	usage.TotalTokens = usage.InputTokens + usage.OutputTokens + usage.CacheReadTokens + usage.CacheCreateTokens
	if account != nil {
		usage.AccountID, usage.AccountType = callAuditAccountSnapshot(account)
	}
	if cost != nil {
		usage.Cost = strconv.FormatFloat(cost.ActualCost, 'f', 10, 64)
		usage.RealCost = strconv.FormatFloat(cost.TotalCost, 'f', 10, 64)
	}
	session.MergeUsage(usage)
}

// callAuditAccountSnapshot preserves the historical CRS dimensions for
// migrated accounts. Native Sub2API accounts fall back to their local ID/type.
func callAuditAccountSnapshot(account *Account) (string, string) {
	if account == nil {
		return "", ""
	}
	accountID := strconv.FormatInt(account.ID, 10)
	accountType := strings.TrimSpace(account.Type)
	if account.Extra == nil {
		return accountID, accountType
	}
	if sourceID, ok := account.Extra["crs_account_id"].(string); ok && strings.TrimSpace(sourceID) != "" {
		accountID = strings.TrimSpace(sourceID)
	}
	if sourceKind, ok := account.Extra["crs_kind"].(string); ok && strings.TrimSpace(sourceKind) != "" {
		accountType = strings.TrimSpace(sourceKind)
	}
	return accountID, accountType
}

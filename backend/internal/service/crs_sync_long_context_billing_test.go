//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type crsLongContextAccountRepo struct {
	AccountRepository
	accounts map[string]*Account
	nextID   int64
}

type crsOpenAILongContextSource struct {
	collection  string
	credentials map[string]any
	extra       map[string]any
	priority    int
}

func newCRSLongContextAccountRepo(existing ...*Account) *crsLongContextAccountRepo {
	repo := &crsLongContextAccountRepo{accounts: make(map[string]*Account)}
	for _, account := range existing {
		if account == nil {
			continue
		}
		crsID, _ := account.Extra["crs_account_id"].(string)
		repo.accounts[crsID] = account
		if account.ID > repo.nextID {
			repo.nextID = account.ID
		}
	}
	return repo
}

func (r *crsLongContextAccountRepo) Create(_ context.Context, account *Account) error {
	r.nextID++
	account.ID = r.nextID
	crsID, _ := account.Extra["crs_account_id"].(string)
	r.accounts[crsID] = account
	return nil
}

func (r *crsLongContextAccountRepo) Update(_ context.Context, account *Account) error {
	crsID, _ := account.Extra["crs_account_id"].(string)
	r.accounts[crsID] = account
	return nil
}

func (r *crsLongContextAccountRepo) GetByCRSAccountID(_ context.Context, crsID string) (*Account, error) {
	return r.accounts[crsID], nil
}

func (r *crsLongContextAccountRepo) ListShadowsByParent(_ context.Context, _ int64) ([]*Account, error) {
	return nil, nil
}

func TestCRSSyncOpenAILongContextBilling(t *testing.T) {
	tests := []struct {
		name          string
		collection    string
		credentials   map[string]any
		sourceExtra   map[string]any
		existingExtra map[string]any
		wantAction    string
		wantEnabled   bool
	}{
		{name: "OAuth create defaults missing value disabled", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, wantAction: "created"},
		{name: "OAuth create preserves source true", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "created", wantEnabled: true},
		{name: "OAuth create preserves source false", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: false}, wantAction: "created"},
		{name: "OAuth update defaults missing value disabled", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, existingExtra: map[string]any{"existing": true}, wantAction: "updated"},
		{name: "OAuth update preserves existing true when source omits value", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "updated", wantEnabled: true},
		{name: "OAuth update preserves existing false when source omits value", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: false}, wantAction: "updated"},
		{name: "OAuth update preserves source true over existing false", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: true}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: false}, wantAction: "updated", wantEnabled: true},
		{name: "OAuth update preserves source false over existing true", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: false}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "updated"},
		{name: "OAuth rejects malformed source value", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: "false"}, wantAction: "failed"},
		{name: "OAuth rejects malformed existing value", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: "false"}, wantAction: "failed"},
		{name: "OAuth update rejects malformed source value", collection: "openaiOAuthAccounts", credentials: map[string]any{"access_token": "oauth-token"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: "false"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "failed"},
		{name: "API key create defaults missing value disabled", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, wantAction: "created"},
		{name: "API key create preserves source true", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "created", wantEnabled: true},
		{name: "API key create preserves source false", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: false}, wantAction: "created"},
		{name: "API key update defaults missing value disabled", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, existingExtra: map[string]any{"existing": true}, wantAction: "updated"},
		{name: "API key update preserves existing true when source omits value", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "updated", wantEnabled: true},
		{name: "API key update preserves existing false when source omits value", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: false}, wantAction: "updated"},
		{name: "API key update preserves source true over existing false", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: true}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: false}, wantAction: "updated", wantEnabled: true},
		{name: "API key update preserves source false over existing true", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: false}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "updated"},
		{name: "API key rejects malformed source value", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: "false"}, wantAction: "failed"},
		{name: "API key rejects malformed existing value", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: "false"}, wantAction: "failed"},
		{name: "API key update rejects malformed source value", collection: "openaiResponsesAccounts", credentials: map[string]any{"api_key": "sk-test"}, sourceExtra: map[string]any{openAILongContextBillingEnabledKey: "false"}, existingExtra: map[string]any{openAILongContextBillingEnabledKey: true}, wantAction: "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const crsID = "crs-openai-1"
			var existing *Account
			if tt.existingExtra != nil {
				existingExtra := mergeMap(tt.existingExtra, map[string]any{"crs_account_id": crsID})
				accountType := AccountTypeOAuth
				if tt.collection == "openaiResponsesAccounts" {
					accountType = AccountTypeAPIKey
				}
				existing = &Account{ID: 41, Platform: PlatformOpenAI, Type: accountType, Extra: existingExtra}
			}
			repo := newCRSLongContextAccountRepo(existing)
			result := runCRSOpenAILongContextSync(t, repo, crsOpenAILongContextSource{
				collection:  tt.collection,
				credentials: tt.credentials,
				extra:       tt.sourceExtra,
			})

			require.Len(t, result.Items, 1)
			require.Equal(t, tt.wantAction, result.Items[0].Action)
			if tt.wantAction == "failed" {
				require.Contains(t, result.Items[0].Error, "openai_long_context_billing_enabled must be a boolean")
				return
			}
			stored, ok := repo.accounts[crsID].Extra[openAILongContextBillingEnabledKey]
			require.True(t, ok)
			require.Equal(t, tt.wantEnabled, stored)
		})
	}
}

func TestCRSSyncWeightModeNeutralizesEveryImportedAccountKind(t *testing.T) {
	repo := newCRSLongContextAccountRepo()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/web/auth/login" {
			_, _ = response.Write([]byte(`{"success":true,"token":"admin-token"}`))
			return
		}
		require.Equal(t, "/admin/sync/export-accounts", request.URL.Path)
		account := func(id string, priority int, credentials map[string]any) map[string]any {
			return map[string]any{
				"kind":        id + "-kind",
				"id":          id,
				"name":        id,
				"isActive":    true,
				"schedulable": true,
				"priority":    priority,
				"credentials": credentials,
			}
		}
		claudeOAuth := account("claude-oauth", 11, map[string]any{"access_token": "claude-token"})
		claudeOAuth["authType"] = AccountTypeOAuth
		openAIOAuth := account("openai-oauth", 33, map[string]any{"access_token": "openai-token"})
		openAIOAuth["authType"] = AccountTypeOAuth
		geminiOAuth := account("gemini-oauth", 55, map[string]any{"refresh_token": "gemini-refresh"})
		geminiOAuth["authType"] = AccountTypeOAuth

		require.NoError(t, json.NewEncoder(response).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"claudeAccounts":          []any{claudeOAuth},
				"claudeConsoleAccounts":   []any{account("claude-console", 22, map[string]any{"api_key": "claude-key"})},
				"openaiOAuthAccounts":     []any{openAIOAuth},
				"openaiResponsesAccounts": []any{account("openai-api-key", 44, map[string]any{"api_key": "openai-key"})},
				"geminiOAuthAccounts":     []any{geminiOAuth},
				"geminiApiKeyAccounts":    []any{account("gemini-api-key", 66, map[string]any{"api_key": "gemini-key"})},
			},
		}))
	}))
	t.Cleanup(server.Close)

	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	refreshOAuth := false
	syncService := NewCRSSyncService(repo, nil, nil, nil, nil, cfg)
	result, err := syncService.SyncFromCRS(context.Background(), SyncFromCRSInput{
		BaseURL:            server.URL,
		Username:           "admin",
		Password:           "password",
		SourcePriorityMode: CRSSourcePriorityModeWeight,
		RefreshOAuth:       &refreshOAuth,
	})
	require.NoError(t, err)
	require.Equal(t, 6, result.Created)
	require.Zero(t, result.Failed)

	wantSourcePriority := map[string]int{
		"claude-oauth": 11, "claude-console": 22, "openai-oauth": 33,
		"openai-api-key": 44, "gemini-oauth": 55, "gemini-api-key": 66,
	}
	for id, sourcePriority := range wantSourcePriority {
		stored := repo.accounts[id]
		require.NotNil(t, stored, "account %s should be imported", id)
		require.Equal(t, crsNeutralPriority, stored.Priority, "account %s must not treat the CRS weight as sub2api priority", id)
		require.Equal(t, sourcePriority, stored.Extra["crs_priority"], "account %s must retain the source value", id)
	}
}

func TestCRSSyncDefaultPriorityModePreservesHistoricalPriority(t *testing.T) {
	repo := newCRSLongContextAccountRepo()
	result := runCRSOpenAILongContextSync(t, repo, crsOpenAILongContextSource{
		collection:  "openaiResponsesAccounts",
		credentials: map[string]any{"api_key": "sk-test"},
		priority:    17,
	})

	require.Equal(t, 1, result.Created)
	stored := repo.accounts["crs-openai-1"]
	require.NotNil(t, stored)
	require.Equal(t, 17, stored.Priority)
	require.NotContains(t, stored.Extra, "crs_priority", "default priority mode must not reinterpret the source field as a weight")
}

func runCRSOpenAILongContextSync(t *testing.T, repo AccountRepository, source crsOpenAILongContextSource) *SyncFromCRSResult {
	t.Helper()
	account := map[string]any{
		"kind":        "openai",
		"id":          "crs-openai-1",
		"name":        "OpenAI CRS",
		"isActive":    true,
		"schedulable": true,
		"priority":    source.priority,
		"credentials": source.credentials,
	}
	if source.extra != nil {
		account["extra"] = source.extra
	}

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/web/auth/login" {
			_, _ = response.Write([]byte(`{"success":true,"token":"admin-token"}`))
			return
		}
		require.Equal(t, "/admin/sync/export-accounts", request.URL.Path)
		require.NoError(t, json.NewEncoder(response).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{source.collection: []any{account}},
		}))
	}))
	t.Cleanup(server.Close)

	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	service := NewCRSSyncService(repo, nil, nil, nil, nil, cfg)
	result, err := service.SyncFromCRS(context.Background(), SyncFromCRSInput{
		BaseURL:  server.URL,
		Username: "admin",
		Password: "password",
	})
	require.NoError(t, err)
	return result
}

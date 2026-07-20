package service

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/apikey"
	"github.com/Wei-Shaw/sub2api/ent/enttest"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "modernc.org/sqlite"
)

func TestNormalizeYunMoStarInputDefaultsAndMetadata(t *testing.T) {
	input, err := normalizeYunMoStarInput(YunMoStarRelayKeyInput{
		ExternalUserID: " 42 ",
		Email:          " User@Example.COM ",
		Username:       " User ",
		Department:     " Platform ",
		Name:           " Codex access ",
		Tags:           []string{"team:ai", "ymstar", ""},
	})
	require.NoError(t, err)
	require.Equal(t, "42", input.ExternalUserID)
	require.Equal(t, "user@example.com", input.Email)
	require.Equal(t, []string{"claude", "gemini", "openai"}, input.Permissions)
	require.Equal(t, []string{"team:ai", "uid:42", "ymstar"}, input.Tags)
	require.Equal(t, "Platform", input.SourceMetadata["department"])
}

func TestNormalizeYunMoStarInputTypedKeyDerivesSinglePlatformPolicy(t *testing.T) {
	input, err := normalizeYunMoStarInput(YunMoStarRelayKeyInput{
		ClientType:     " Claude_Code ",
		ExternalUserID: "42",
		Email:          "user@example.com",
		Name:           "Claude Code access",
		Permissions:    []string{"openai", "gemini"},
	})
	require.NoError(t, err)
	require.Equal(t, YunMoStarClientClaudeCode, input.ClientType)
	require.Equal(t, []string{YunMoStarPermissionClaude}, input.Permissions)
	require.Contains(t, input.Tags, "client:claude_code")
	require.Equal(t, YunMoStarClientClaudeCode, input.SourceMetadata["client_type"])
}

func TestNormalizeYunMoStarInputRejectsUnknownClientType(t *testing.T) {
	_, err := normalizeYunMoStarInput(YunMoStarRelayKeyInput{
		ClientType:     "cursor",
		ExternalUserID: "42",
		Email:          "user@example.com",
		Name:           "relay key",
	})
	require.ErrorContains(t, err, "unsupported client_type")
}

func TestYunMoStarClientGroupPolicyUsesExplicitMappings(t *testing.T) {
	svc := &YunMoStarIntegrationService{config: config.YunMoStarIntegrationConfig{
		CodexGroupID:      6,
		ClaudeCodeGroupID: 5,
		GeminiGroupID:     7,
	}}
	tests := []struct {
		client   string
		groupID  int64
		platform string
	}{
		{YunMoStarClientCodex, 6, PlatformOpenAI},
		{YunMoStarClientClaudeCode, 5, PlatformAnthropic},
		{YunMoStarClientGemini, 7, PlatformGemini},
		{"", 6, PlatformOpenAI},
	}
	for _, tt := range tests {
		t.Run(tt.client, func(t *testing.T) {
			groupID, platform, err := svc.clientGroupPolicy(tt.client)
			require.NoError(t, err)
			require.Equal(t, tt.groupID, groupID)
			require.Equal(t, tt.platform, platform)
		})
	}
}

func newYunMoStarIntegrationTestService(t *testing.T) (*YunMoStarIntegrationService, *dbent.Client, *config.Config) {
	t.Helper()
	dsn := fmt.Sprintf("file:yunmostar_integration_%s?mode=memory&cache=shared&_fk=1", t.Name())
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	require.NoError(t, err)
	drv := entsql.OpenDB(dialect.SQLite, db)
	client := enttest.NewClient(t, enttest.WithOptions(dbent.Driver(drv)))
	t.Cleanup(func() { _ = client.Close() })
	cfg := &config.Config{Default: config.DefaultConfig{APIKeyPrefix: "cr_"}}
	keySvc := NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg)
	return NewYunMoStarIntegrationService(client, keySvc, cfg), client, cfg
}

func createYunMoStarTestGroup(t *testing.T, client *dbent.Client, name, platform, status string) int64 {
	t.Helper()
	group, err := client.Group.Create().
		SetName(name).
		SetPlatform(platform).
		SetStatus(status).
		Save(context.Background())
	require.NoError(t, err)
	return group.ID
}

func TestYunMoStarIntegrationTypedImportIsIdempotentAndPreservesCustomKey(t *testing.T) {
	svc, client, cfg := newYunMoStarIntegrationTestService(t)
	ctx := context.Background()
	openAIGroupID := createYunMoStarTestGroup(t, client, "openai", PlatformOpenAI, StatusActive)
	cfg.YunMoStarIntegration.CodexGroupID = openAIGroupID
	svc.config = cfg.YunMoStarIntegration

	input := YunMoStarRelayKeyInput{
		ClientType:     YunMoStarClientCodex,
		ExternalUserID: "42",
		Email:          "holder@example.com",
		Username:       "Holder",
		Name:           "Codex access",
		Permissions:    []string{YunMoStarPermissionClaude, PlatformGemini},
		CustomKey:      "cr_preserved_exact_key_123456",
	}
	first, err := svc.Import(ctx, "legacy-source-id", input)
	require.NoError(t, err)
	second, err := svc.Import(ctx, "legacy-source-id", input)
	require.NoError(t, err)
	require.Equal(t, first.APIKeyID, second.APIKeyID)
	require.Equal(t, openAIGroupID, second.GroupID)
	require.Equal(t, PlatformOpenAI, second.GroupPlatform)
	require.Equal(t, []string{PlatformOpenAI}, second.Permissions)

	stored, err := client.APIKey.Query().Where(apikey.IDEQ(second.APIKeyID)).Only(ctx)
	require.NoError(t, err)
	require.Equal(t, input.CustomKey, stored.Key)
	require.Equal(t, []string{PlatformOpenAI}, stored.Permissions)
	require.NotNil(t, stored.GroupID)
	require.Equal(t, openAIGroupID, *stored.GroupID)
}

func TestYunMoStarIntegrationTypedRequestFailsForMissingOrWrongGroup(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testing.T, *dbent.Client, *config.Config)
	}{
		{name: "missing id"},
		{
			name: "wrong platform",
			configure: func(t *testing.T, client *dbent.Client, cfg *config.Config) {
				cfg.YunMoStarIntegration.CodexGroupID = createYunMoStarTestGroup(t, client, "anthropic", PlatformAnthropic, StatusActive)
			},
		},
		{
			name: "inactive",
			configure: func(t *testing.T, client *dbent.Client, cfg *config.Config) {
				cfg.YunMoStarIntegration.CodexGroupID = createYunMoStarTestGroup(t, client, "inactive", PlatformOpenAI, StatusDisabled)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, client, cfg := newYunMoStarIntegrationTestService(t)
			if tt.configure != nil {
				tt.configure(t, client, cfg)
			}
			svc.config = cfg.YunMoStarIntegration
			_, err := svc.Create(context.Background(), YunMoStarRelayKeyInput{
				ClientType:     YunMoStarClientCodex,
				ExternalUserID: "42",
				Email:          "holder@example.com",
				Username:       "Holder",
				Name:           "Codex access",
			})
			require.ErrorIs(t, err, ErrExternalInputInvalid)
			exists, countErr := client.APIKey.Query().Exist(context.Background())
			require.NoError(t, countErr)
			require.False(t, exists)
		})
	}
}

func TestNormalizeYunMoStarInputRejectsUnknownPermission(t *testing.T) {
	_, err := normalizeYunMoStarInput(YunMoStarRelayKeyInput{
		ExternalUserID: "42",
		Email:          "user@example.com",
		Name:           "relay key",
		Permissions:    []string{"openai", "bedrock"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported permission")
}

func TestAPIKeyAllowsPlatform(t *testing.T) {
	require.True(t, (&APIKey{}).AllowsPlatform(PlatformGemini), "legacy keys stay unrestricted")
	key := &APIKey{Permissions: []string{" Claude ", "OPENAI"}}
	require.True(t, key.AllowsPlatform(YunMoStarPermissionClaude))
	require.True(t, key.AllowsPlatform(PlatformOpenAI))
	require.False(t, key.AllowsPlatform(PlatformGemini))
}

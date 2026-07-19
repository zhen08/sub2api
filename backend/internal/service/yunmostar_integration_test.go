package service

import (
	"testing"

	"github.com/stretchr/testify/require"
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

package service

import (
	"context"
	"errors"
	"fmt"
)

const userOpenAIModelPolicyPrefix = "user_openai_model_policy:"

func validUserOpenAIModelPolicy(level string) bool {
	return level == "terra" || level == "luna" || level == "full"
}

// GetUserOpenAIModelPolicy deliberately reads persistence on every invocation.
// Neither the authentication cache nor long-lived WS sessions own policy snapshots.
func (s *SettingService) GetUserOpenAIModelPolicy(ctx context.Context, userID int64) (string, error) {
	if userID <= 0 {
		return "", fmt.Errorf("invalid user ID")
	}
	if s == nil || s.settingRepo == nil {
		return "", fmt.Errorf("user model policy store unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, gatewayForwardingDBTimeout)
	defer cancel()
	value, err := s.settingRepo.GetValue(ctx, fmt.Sprintf("%s%d", userOpenAIModelPolicyPrefix, userID))
	if errors.Is(err, ErrSettingNotFound) {
		return "original", nil
	}
	if err != nil {
		return "", fmt.Errorf("read user model policy: %w", err)
	}
	if !validUserOpenAIModelPolicy(value) {
		return "", fmt.Errorf("invalid persisted user model policy")
	}
	return value, nil
}

func (s *SettingService) SetUserOpenAIModelPolicy(ctx context.Context, userID int64, level string) (string, error) {
	if userID <= 0 || !validUserOpenAIModelPolicy(level) {
		return "", fmt.Errorf("invalid user model policy")
	}
	if s == nil || s.settingRepo == nil {
		return "", fmt.Errorf("user model policy store unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, gatewayForwardingDBTimeout)
	defer cancel()
	if err := s.settingRepo.Set(ctx, fmt.Sprintf("%s%d", userOpenAIModelPolicyPrefix, userID), level); err != nil {
		return "", err
	}
	actual, err := s.GetUserOpenAIModelPolicy(ctx, userID)
	if err != nil {
		return "", err
	}
	if actual != level {
		return "", fmt.Errorf("user model policy readback mismatch")
	}
	return actual, nil
}

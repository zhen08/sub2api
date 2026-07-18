package callaudit

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

func NewScope(input ScopeInput, retentionDays int, now time.Time) (Scope, error) {
	if retentionDays < 1 {
		return Scope{}, fmt.Errorf("retention days must be positive")
	}
	if now.IsZero() {
		now = time.Now()
	}
	requestID := strings.TrimSpace(input.RequestID)
	if requestID == "" {
		requestID = uuid.NewString()
	}
	if len(requestID) > 512 {
		return Scope{}, fmt.Errorf("request id exceeds 512 bytes")
	}
	startedAt := input.RequestStartedAt
	if startedAt.IsZero() {
		startedAt = now
	}
	protocol := input.Protocol
	if protocol == "" {
		protocol = ProtocolUnknown
	}
	return Scope{
		RequestID:        requestID,
		CreatedAt:        now.UTC(),
		RequestStartedAt: startedAt.UTC(),
		RetentionUntil:   startedAt.UTC().AddDate(0, 0, retentionDays),
		Endpoint:         NormalizePath(input.Endpoint),
		Method:           strings.ToUpper(strings.TrimSpace(input.Method)),
		Protocol:         protocol,
		Identity:         input.Identity,
		Stream:           input.Stream,
	}, nil
}

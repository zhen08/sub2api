// Package callaudit contains the relay-independent core of full AI call auditing.
package callaudit

import "time"

const (
	LegacyCallsTable     = "audit_calls"
	LegacyArtifactsTable = "audit_artifacts"
	LegacyObjectPrefix   = "ai-call-audit"

	S3MetadataRequestID    = "request-id"
	S3MetadataArtifactKind = "artifact-kind"
)

type Protocol string

const (
	ProtocolUnknown      Protocol = "unknown"
	ProtocolAnthropic    Protocol = "anthropic"
	ProtocolOpenAI       Protocol = "openai"
	ProtocolOpenAIClaude Protocol = "openai-claude"
	ProtocolGemini       Protocol = "gemini"
	ProtocolGeminiCLI    Protocol = "gemini-cli"
	ProtocolAntigravity  Protocol = "antigravity"
	ProtocolAzureOpenAI  Protocol = "azure-openai"
	ProtocolDroid        Protocol = "droid"
)

type ArtifactKind string

const (
	ArtifactClientRequest   ArtifactKind = "client_request"
	ArtifactUpstreamRequest ArtifactKind = "upstream_request"
	ArtifactResponse        ArtifactKind = "response"
)

func (k ArtifactKind) Valid() bool {
	switch k {
	case ArtifactClientRequest, ArtifactUpstreamRequest, ArtifactResponse:
		return true
	default:
		return false
	}
}

type CallStatus string

const (
	CallStatusOK      CallStatus = "ok"
	CallStatusError   CallStatus = "error"
	CallStatusAborted CallStatus = "aborted"
)

type CaptureStatus string

const (
	CapturePending  CaptureStatus = "pending"
	CaptureStored   CaptureStatus = "stored"
	CaptureRetrying CaptureStatus = "retrying"
	CaptureFailed   CaptureStatus = "failed"
)

type ManifestState string

const (
	ManifestReady      ManifestState = "ready"
	ManifestProcessing ManifestState = "processing"
	ManifestRetry      ManifestState = "retry"
	ManifestDeadLetter ManifestState = "dead_letter"
)

// IdentitySnapshot deliberately stores display values at request time. Historical
// audit queries must not depend on mutable user or API-key rows.
type IdentitySnapshot struct {
	APIKeyID      string `json:"apiKeyId,omitempty"`
	APIKeyName    string `json:"apiKeyName,omitempty"`
	UserID        string `json:"userId,omitempty"`
	UserUsername  string `json:"userUsername,omitempty"`
	GroupID       string `json:"groupId,omitempty"`
	GroupName     string `json:"groupName,omitempty"`
	GroupPlatform string `json:"groupPlatform,omitempty"`
}

type Scope struct {
	RequestID        string
	CreatedAt        time.Time
	RequestStartedAt time.Time
	RetentionUntil   time.Time
	Endpoint         string
	Method           string
	Protocol         Protocol
	Identity         IdentitySnapshot
	Stream           bool
}

type ScopeInput struct {
	RequestID        string
	RequestStartedAt time.Time
	Endpoint         string
	Method           string
	Protocol         Protocol
	Identity         IdentitySnapshot
	Stream           bool
}

// Usage is a cumulative snapshot. Merge keeps monotonic token counters and the
// latest non-empty routing/cost values, which is safe for concurrent partial
// usage callbacks without double-counting cumulative provider events.
type Usage struct {
	AccountID         string `json:"accountId,omitempty"`
	AccountType       string `json:"accountType,omitempty"`
	Model             string `json:"model,omitempty"`
	InputTokens       int64  `json:"inputTokens,omitempty"`
	OutputTokens      int64  `json:"outputTokens,omitempty"`
	CacheReadTokens   int64  `json:"cacheReadTokens,omitempty"`
	CacheCreateTokens int64  `json:"cacheCreateTokens,omitempty"`
	TotalTokens       int64  `json:"totalTokens,omitempty"`
	Cost              string `json:"cost,omitempty"`
	RealCost          string `json:"realCost,omitempty"`
}

func (u *Usage) Merge(update Usage) {
	if update.AccountID != "" {
		u.AccountID = update.AccountID
	}
	if update.AccountType != "" {
		u.AccountType = update.AccountType
	}
	if update.Model != "" {
		u.Model = update.Model
	}
	u.InputTokens = max(u.InputTokens, update.InputTokens)
	u.OutputTokens = max(u.OutputTokens, update.OutputTokens)
	u.CacheReadTokens = max(u.CacheReadTokens, update.CacheReadTokens)
	u.CacheCreateTokens = max(u.CacheCreateTokens, update.CacheCreateTokens)
	u.TotalTokens = max(u.TotalTokens, update.TotalTokens)
	if update.Cost != "" {
		u.Cost = update.Cost
	}
	if update.RealCost != "" {
		u.RealCost = update.RealCost
	}
}

type ArtifactRef struct {
	Kind        ArtifactKind `json:"kind"`
	Sequence    int          `json:"sequence"`
	SpoolPath   string       `json:"spoolPath"`
	ContentType string       `json:"contentType"`
	Bytes       int64        `json:"bytes,omitempty"`
}

// Manifest intentionally keeps the legacy camelCase wire names so an uploader
// can write the existing audit_calls/audit_artifacts schema without translation.
type Manifest struct {
	Version          int            `json:"version"`
	State            ManifestState  `json:"state"`
	RequestID        string         `json:"requestId"`
	CreatedAt        time.Time      `json:"createdAt"`
	RequestStartedAt time.Time      `json:"requestStartedAt,omitempty"`
	RetentionUntil   time.Time      `json:"retentionUntil"`
	Endpoint         string         `json:"endpoint,omitempty"`
	Method           string         `json:"method,omitempty"`
	Protocol         Protocol       `json:"protocol,omitempty"`
	APIKeyID         string         `json:"apiKeyId,omitempty"`
	APIKeyName       string         `json:"apiKeyName,omitempty"`
	UserID           string         `json:"userId,omitempty"`
	UserUsername     string         `json:"userUsername,omitempty"`
	Status           CallStatus     `json:"status"`
	StatusCode       *int           `json:"statusCode,omitempty"`
	Stream           bool           `json:"stream"`
	CaptureStatus    CaptureStatus  `json:"captureStatus"`
	Usage            Usage          `json:"usage,omitempty"`
	Artifacts        []ArtifactRef  `json:"artifacts"`
	Meta             map[string]any `json:"meta,omitempty"`
	EventSpoolPath   string         `json:"eventSpoolPath,omitempty"`
	Attempts         int            `json:"attempt,omitempty"`
	NextAttemptAt    *time.Time     `json:"nextAttemptAt,omitempty"`
	ClaimedAt        *time.Time     `json:"claimedAt,omitempty"`
	LastAttemptAt    *time.Time     `json:"lastAttemptAt,omitempty"`
	FailedAt         *time.Time     `json:"failedAt,omitempty"`
	LastErrorCode    string         `json:"lastErrorCode,omitempty"`
}

type Outcome struct {
	Status            CallStatus
	StatusCode        *int
	TerminationReason string
	Meta              map[string]any
}

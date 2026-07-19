package service

import (
	"context"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/apikey"
	"github.com/Wei-Shaw/sub2api/ent/authidentity"
	dbgroup "github.com/Wei-Shaw/sub2api/ent/group"
	dbuser "github.com/Wei-Shaw/sub2api/ent/user"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/google/uuid"
)

const YunMoStarSource = "yunmostar"
const YunMoStarPermissionClaude = "claude"

var (
	ErrExternalIdentityConflict = infraerrors.Conflict("EXTERNAL_IDENTITY_CONFLICT", "external user or API key conflicts with an existing record")
	ErrExternalInputInvalid     = infraerrors.BadRequest("EXTERNAL_INPUT_INVALID", "invalid YunMoProject relay key input")
	defaultYunMoStarPermissions = []string{YunMoStarPermissionClaude, PlatformOpenAI, PlatformGemini}
)

// YunMoStarRelayKeyInput contains only non-secret profile metadata plus the
// optional existing key used by the one-time migration endpoint.
type YunMoStarRelayKeyInput struct {
	ExternalUserID string         `json:"external_user_id"`
	Email          string         `json:"email"`
	Username       string         `json:"username"`
	Department     string         `json:"department"`
	Name           string         `json:"name"`
	Tags           []string       `json:"tags"`
	Permissions    []string       `json:"permissions"`
	SourceMetadata map[string]any `json:"source_metadata"`
	CustomKey      string         `json:"key,omitempty"`
}

type YunMoStarRelayKeyResult struct {
	SourceKeyID string   `json:"id"`
	APIKeyID    int64    `json:"api_key_id"`
	UserID      int64    `json:"user_id"`
	Name        string   `json:"name"`
	Prefix      string   `json:"prefix"`
	Tags        []string `json:"tags"`
	Permissions []string `json:"permissions"`
	Key         string   `json:"api_key,omitempty"`
}

type YunMoStarIntegrationService struct {
	client        *dbent.Client
	apiKeyService *APIKeyService
}

func NewYunMoStarIntegrationService(client *dbent.Client, apiKeyService *APIKeyService) *YunMoStarIntegrationService {
	return &YunMoStarIntegrationService{client: client, apiKeyService: apiKeyService}
}

func normalizeYunMoStarInput(input YunMoStarRelayKeyInput) (YunMoStarRelayKeyInput, error) {
	input.ExternalUserID = strings.TrimSpace(input.ExternalUserID)
	input.Email = strings.ToLower(strings.TrimSpace(input.Email))
	input.Username = strings.TrimSpace(input.Username)
	input.Department = strings.TrimSpace(input.Department)
	input.Name = strings.TrimSpace(input.Name)
	input.CustomKey = strings.TrimSpace(input.CustomKey)
	if input.ExternalUserID == "" || input.Email == "" || !strings.Contains(input.Email, "@") || input.Name == "" {
		return input, ErrExternalInputInvalid
	}

	permissions := input.Permissions
	if len(permissions) == 0 {
		permissions = defaultYunMoStarPermissions
	}
	allowed := map[string]bool{YunMoStarPermissionClaude: true, PlatformOpenAI: true, PlatformGemini: true}
	permissionSet := make(map[string]struct{}, len(permissions))
	for _, permission := range permissions {
		permission = strings.ToLower(strings.TrimSpace(permission))
		if !allowed[permission] {
			return input, fmt.Errorf("%w: unsupported permission %q", ErrExternalInputInvalid, permission)
		}
		permissionSet[permission] = struct{}{}
	}
	input.Permissions = input.Permissions[:0]
	for permission := range permissionSet {
		input.Permissions = append(input.Permissions, permission)
	}
	sort.Strings(input.Permissions)

	tags := make(map[string]struct{}, len(input.Tags)+2)
	tags["ymstar"] = struct{}{}
	tags["uid:"+input.ExternalUserID] = struct{}{}
	for _, tag := range input.Tags {
		if tag = strings.TrimSpace(tag); tag != "" {
			tags[tag] = struct{}{}
		}
	}
	input.Tags = input.Tags[:0]
	for tag := range tags {
		input.Tags = append(input.Tags, tag)
	}
	sort.Strings(input.Tags)
	if input.SourceMetadata == nil {
		input.SourceMetadata = map[string]any{}
	}
	if input.Department != "" {
		input.SourceMetadata["department"] = input.Department
	}
	return input, nil
}

func (s *YunMoStarIntegrationService) Create(ctx context.Context, input YunMoStarRelayKeyInput) (*YunMoStarRelayKeyResult, error) {
	return s.upsert(ctx, "ymstar-"+uuid.NewString(), input, true)
}

func (s *YunMoStarIntegrationService) Import(ctx context.Context, sourceKeyID string, input YunMoStarRelayKeyInput) (*YunMoStarRelayKeyResult, error) {
	sourceKeyID = strings.TrimSpace(sourceKeyID)
	if sourceKeyID == "" || strings.TrimSpace(input.CustomKey) == "" {
		return nil, ErrExternalInputInvalid
	}
	return s.upsert(ctx, sourceKeyID, input, false)
}

func (s *YunMoStarIntegrationService) upsert(ctx context.Context, sourceKeyID string, input YunMoStarRelayKeyInput, revealKey bool) (*YunMoStarRelayKeyResult, error) {
	input, err := normalizeYunMoStarInput(input)
	if err != nil {
		return nil, err
	}
	if input.CustomKey != "" {
		if err := s.apiKeyService.ValidateCustomKey(input.CustomKey); err != nil {
			return nil, err
		}
	}

	tx, err := s.client.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	txCtx := dbent.NewTxContext(ctx, tx)
	client := tx.Client()

	// YunMoProject relay keys are multi-protocol keys. Sub2API still requires
	// every key to belong to one active group, so anchor them to the deployment's
	// OpenAI group and use explicit protocol routes for Gemini/Claude. Keeping
	// this lookup inside the transaction makes create/update fail closed when the
	// required routing group has not been provisioned yet.
	targetGroup, err := client.Group.Query().Where(
		dbgroup.PlatformEQ(PlatformOpenAI),
		dbgroup.StatusEQ(StatusActive),
		dbgroup.DeletedAtIsNil(),
	).Order(dbent.Asc(dbgroup.FieldID)).First(txCtx)
	if err != nil {
		return nil, fmt.Errorf("resolve YunMoProject routing group: %w", err)
	}

	userEntity, err := client.User.Query().Where(
		dbuser.SourceEQ(YunMoStarSource),
		dbuser.SourceIDEQ(input.ExternalUserID),
		dbuser.DeletedAtIsNil(),
	).Only(txCtx)
	if err != nil && !dbent.IsNotFound(err) {
		return nil, err
	}
	if dbent.IsNotFound(err) {
		conflict, findErr := client.User.Query().Where(dbuser.EmailEqualFold(input.Email), dbuser.DeletedAtIsNil()).Exist(txCtx)
		if findErr != nil {
			return nil, findErr
		}
		if conflict {
			return nil, ErrExternalIdentityConflict
		}
		managedUser := &User{
			Email:          input.Email,
			Username:       input.Username,
			Notes:          "Managed by YunMoProject",
			Source:         YunMoStarSource,
			SourceID:       input.ExternalUserID,
			SourceMetadata: input.SourceMetadata,
			Role:           RoleUser,
			Concurrency:    5,
			Status:         StatusActive,
			SignupSource:   "email",
		}
		if err := managedUser.SetPassword(uuid.NewString() + uuid.NewString()); err != nil {
			return nil, err
		}
		userEntity, err = client.User.Create().
			SetEmail(managedUser.Email).
			SetUsername(managedUser.Username).
			SetNotes(managedUser.Notes).
			SetPasswordHash(managedUser.PasswordHash).
			SetRole(managedUser.Role).
			SetBalance(managedUser.Balance).
			SetConcurrency(managedUser.Concurrency).
			SetStatus(managedUser.Status).
			SetSignupSource(managedUser.SignupSource).
			SetSource(managedUser.Source).
			SetSourceID(managedUser.SourceID).
			SetSourceMetadata(managedUser.SourceMetadata).
			Save(txCtx)
		if err != nil {
			return nil, err
		}
		if err := client.AuthIdentity.Create().
			SetUserID(userEntity.ID).
			SetProviderType("email").
			SetProviderKey("email").
			SetProviderSubject(input.Email).
			SetVerifiedAt(time.Now().UTC()).
			SetMetadata(map[string]any{"source": YunMoStarSource}).
			OnConflictColumns(authidentity.FieldProviderType, authidentity.FieldProviderKey, authidentity.FieldProviderSubject).
			DoNothing().
			Exec(txCtx); err != nil {
			return nil, err
		}
	} else {
		if !strings.EqualFold(userEntity.Email, input.Email) {
			return nil, ErrExternalIdentityConflict
		}
		userEntity, err = userEntity.Update().
			SetUsername(input.Username).
			SetSourceMetadata(input.SourceMetadata).
			SetStatus(StatusActive).
			Save(txCtx)
		if err != nil {
			return nil, err
		}
	}

	keyEntity, err := client.APIKey.Query().Where(
		apikey.SourceEQ(YunMoStarSource),
		apikey.SourceIDEQ(sourceKeyID),
		apikey.DeletedAtIsNil(),
	).Only(txCtx)
	if err != nil && !dbent.IsNotFound(err) {
		return nil, err
	}
	created := false
	if dbent.IsNotFound(err) {
		keyValue := input.CustomKey
		if keyValue == "" {
			keyValue, err = s.apiKeyService.GenerateKey()
			if err != nil {
				return nil, err
			}
		}
		existing, findErr := client.APIKey.Query().Where(apikey.KeyEQ(keyValue), apikey.DeletedAtIsNil()).Only(txCtx)
		if findErr == nil {
			if existing.UserID != userEntity.ID || existing.Source != "" {
				return nil, ErrExternalIdentityConflict
			}
			keyEntity, err = existing.Update().
				SetName(html.EscapeString(input.Name)).
				SetGroupID(targetGroup.ID).
				SetSource(YunMoStarSource).
				SetSourceID(sourceKeyID).
				SetTags(input.Tags).
				SetPermissions(input.Permissions).
				SetStatus(StatusActive).
				Save(txCtx)
		} else if dbent.IsNotFound(findErr) {
			keyEntity, err = client.APIKey.Create().
				SetUserID(userEntity.ID).
				SetKey(keyValue).
				SetName(html.EscapeString(input.Name)).
				SetGroupID(targetGroup.ID).
				SetStatus(StatusActive).
				SetSource(YunMoStarSource).
				SetSourceID(sourceKeyID).
				SetTags(input.Tags).
				SetPermissions(input.Permissions).
				Save(txCtx)
			created = true
		} else {
			return nil, findErr
		}
		if err != nil {
			return nil, err
		}
	} else {
		if keyEntity.UserID != userEntity.ID || (input.CustomKey != "" && keyEntity.Key != input.CustomKey) {
			return nil, ErrExternalIdentityConflict
		}
		keyEntity, err = keyEntity.Update().
			SetName(html.EscapeString(input.Name)).
			SetGroupID(targetGroup.ID).
			SetTags(input.Tags).
			SetPermissions(input.Permissions).
			SetStatus(StatusActive).
			Save(txCtx)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.apiKeyService.InvalidateAuthCacheByKey(ctx, keyEntity.Key)
	result := &YunMoStarRelayKeyResult{
		SourceKeyID: sourceKeyID,
		APIKeyID:    keyEntity.ID,
		UserID:      userEntity.ID,
		Name:        keyEntity.Name,
		Prefix:      keyPrefix(keyEntity.Key),
		Tags:        input.Tags,
		Permissions: input.Permissions,
	}
	if revealKey && created {
		result.Key = keyEntity.Key
	}
	return result, nil
}

func (s *YunMoStarIntegrationService) Delete(ctx context.Context, sourceKeyID string) error {
	sourceKeyID = strings.TrimSpace(sourceKeyID)
	if sourceKeyID == "" {
		return ErrExternalInputInvalid
	}
	keyEntity, err := s.client.APIKey.Query().Where(
		apikey.SourceEQ(YunMoStarSource),
		apikey.SourceIDEQ(sourceKeyID),
		apikey.DeletedAtIsNil(),
	).Only(ctx)
	if dbent.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.apiKeyService.Delete(ctx, keyEntity.ID, keyEntity.UserID)
}

func keyPrefix(key string) string {
	if len(key) <= 12 {
		return key
	}
	return key[:12]
}

package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type openAIUserModelPolicyContextKey struct{}
type openAIUserModelPolicyIdentity struct {
	userID  int64
	groupID *int64
}

// WithOpenAIUserModelPolicy carries immutable authentication identifiers only.
// Every decision reads the mutable policy from durable storage.
func WithOpenAIUserModelPolicy(ctx context.Context, userID int64, groupID *int64) context.Context {
	var group *int64
	if groupID != nil {
		id := *groupID
		group = &id
	}
	return context.WithValue(ctx, openAIUserModelPolicyContextKey{}, openAIUserModelPolicyIdentity{userID, group})
}
func (s *OpenAIGatewayService) userOpenAIModelPolicy(ctx context.Context) (string, error) {
	identity, ok := ctx.Value(openAIUserModelPolicyContextKey{}).(openAIUserModelPolicyIdentity)
	if !ok {
		return "original", nil
	} // internal/admin probes have no authenticated user
	return s.settingService.GetUserOpenAIModelPolicy(ctx, identity.userID)
}

// ResolveUserOpenAIChannelMapping caps before scheduling and again after channel
// rewriting. Full recovery bypasses remapping only; restriction checks remain.
func (s *OpenAIGatewayService) ResolveUserOpenAIChannelMapping(ctx context.Context, groupID *int64, model string) (ChannelMappingResult, error) {
	level, err := s.userOpenAIModelPolicy(ctx)
	if err != nil {
		return ChannelMappingResult{}, err
	}
	clamped := clampUserOpenAIModel(level, model)
	mapping, _ := s.ResolveChannelMappingAndRestrict(ctx, groupID, clamped)
	if level == "full" {
		mapping.MappedModel = clamped
	} else {
		mapping.MappedModel = clampUserOpenAIModel(level, mapping.MappedModel)
	}
	mapping.Mapped = mapping.MappedModel != model
	return mapping, nil
}

func (s *OpenAIGatewayService) ClampUserOpenAIModel(ctx context.Context, model string) (string, error) {
	level, err := s.userOpenAIModelPolicy(ctx)
	if err != nil {
		return "", err
	}
	return clampUserOpenAIModel(level, model), nil
}

func clampUserOpenAIModel(level, model string) string {
	if level == "original" {
		return model
	}
	canonical := normalizeKnownOpenAICodexModel(model)
	if isOpenAIGPT6AstraModel(model) {
		canonical = "gpt-6-astra"
	}
	// Policy tiers are not version ordering: both Luna models are below Terra.
	// Preserve explicit legacy Luna requests; only higher tiers use the new cap.
	rank := map[string]int{"gpt-6-astra": 4, "gpt-5.6-sol": 3, "gpt-5.6-terra": 2, "gpt-5.6-luna": 1, "gpt-6-luna": 1}[canonical]
	if rank == 0 {
		return model
	}
	if level == "terra" && rank > 2 {
		return "gpt-5.6-terra"
	}
	if level == "luna" && rank > 1 {
		return "gpt-6-luna"
	}
	return canonical
}

func (s *OpenAIGatewayService) enforceUserOpenAIModelBody(ctx context.Context, account *Account, body []byte) ([]byte, error) {
	level, err := s.userOpenAIModelPolicy(ctx)
	if err != nil {
		return nil, err
	}
	return s.enforceUserOpenAIModelBodyLevel(ctx, account, body, level)
}
func (s *OpenAIGatewayService) finalizeUserOpenAIModelBody(ctx context.Context, account *Account, body []byte) ([]byte, error) {
	level, err := s.userOpenAIModelPolicy(ctx)
	if err != nil {
		return nil, err
	}
	out, err := s.enforceUserOpenAIModelBodyLevel(ctx, account, body, level)
	if err == nil && level != "original" {
		observeUserModelDispatch(ctx, body, out, level)
	}
	return out, err
}
func (s *OpenAIGatewayService) enforceUserOpenAIModelBodyLevel(ctx context.Context, account *Account, body []byte, level string) ([]byte, error) {
	var err error
	if level == "original" {
		return body, nil
	}
	if !gjson.ValidBytes(body) {
		return nil, fmt.Errorf("invalid model policy request JSON")
	}
	if err := validateUserPolicyModelFields(gjson.ParseBytes(body)); err != nil {
		return nil, err
	}
	for _, path := range []string{"model", "session.model", "response.model"} {
		field := gjson.GetBytes(body, path)
		if !field.Exists() {
			continue
		}
		model := field.String()
		clamped := clampUserOpenAIModel(level, model)
		if clamped == model {
			continue
		}
		if account != nil && !account.IsModelSupported(clamped) {
			return nil, fmt.Errorf("policy model is unavailable on selected account")
		}
		if identity, ok := ctx.Value(openAIUserModelPolicyContextKey{}).(openAIUserModelPolicyIdentity); ok && identity.groupID != nil && s.channelService != nil && s.channelService.IsModelRestricted(ctx, *identity.groupID, clamped) {
			return nil, fmt.Errorf("policy model is restricted by channel")
		}
		body, err = sjson.SetBytes(body, path, clamped)
		if err != nil {
			return nil, err
		}
	}
	return body, nil
}

func validateUserPolicyModelFields(object gjson.Result) error {
	if !object.IsObject() {
		return fmt.Errorf("model policy requires a JSON object")
	}
	seen := map[string]bool{}
	var invalid error
	object.ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		lower := strings.ToLower(name)
		if lower != "model" && lower != "session" && lower != "response" {
			return true
		}
		if seen[lower] || lower != name {
			invalid = fmt.Errorf("ambiguous model field")
			return false
		}
		seen[lower] = true
		if lower == "model" {
			if value.Type != gjson.String || strings.TrimSpace(value.String()) == "" {
				invalid = fmt.Errorf("invalid model field")
				return false
			}
		} else if value.IsObject() {
			invalid = validateUserPolicyModelFields(value)
			if invalid != nil {
				return false
			}
		}
		return true
	})
	return invalid
}

// Last HTTP gate, shared by Responses, Chat, Messages, their passthrough and
// converted protocol paths, retries and the WebSocket-to-HTTP bridge.
func (s *OpenAIGatewayService) finalizeUserOpenAIHTTPRequest(request *http.Request, account *Account) ([]byte, string, error) {
	if request == nil || request.Body == nil || request.Method != http.MethodPost {
		return nil, "", nil
	}
	if _, ok := request.Context().Value(openAIUserModelPolicyContextKey{}).(openAIUserModelPolicyIdentity); !ok {
		return nil, "", nil
	}
	// Only JSON inference bodies contain text model selection. Multipart image
	// endpoints cannot execute the capped text models.
	textInference := strings.HasSuffix(request.URL.Path, "/responses") || strings.HasSuffix(request.URL.Path, "/responses/compact") || strings.HasSuffix(request.URL.Path, "/chat/completions") || strings.HasSuffix(request.URL.Path, "/messages")
	if !textInference && !strings.Contains(request.Header.Get("Content-Type"), "json") {
		return nil, "", nil
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, "", err
	}
	_ = request.Body.Close()
	body, err = s.finalizeUserOpenAIModelBody(request.Context(), account, body)
	if err != nil {
		return nil, "", err
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return body, strings.TrimSpace(gjson.GetBytes(body, "model").String()), nil
}

func (s *OpenAIGatewayService) enforceUserOpenAIHTTPRequest(request *http.Request, account *Account) error {
	_, _, err := s.finalizeUserOpenAIHTTPRequest(request, account)
	return err
}

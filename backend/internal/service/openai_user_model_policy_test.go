package service

import (
	"bytes"
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"sync"
	"testing"
)

type userModelPolicyRepo struct {
	SettingRepository
	mu     sync.Mutex
	values map[string]string
	err    error
}

func (r *userModelPolicyRepo) GetValue(_ context.Context, key string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return "", r.err
	}
	v, ok := r.values[key]
	if !ok {
		return "", ErrSettingNotFound
	}
	return v, nil
}
func (r *userModelPolicyRepo) Set(_ context.Context, key, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.values[key] = value
	return nil
}

type policyCaptureUpstream struct {
	HTTPUpstream
	models []string
}

func (u *policyCaptureUpstream) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	b, e := io.ReadAll(r.Body)
	if e != nil {
		return nil, e
	}
	u.models = append(u.models, gjson.GetBytes(b, "model").String())
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader([]byte(`{}`))), Header: http.Header{}}, nil
}
func TestOpenAIUserModelPolicyFinalHTTP(t *testing.T) {
	repo := &userModelPolicyRepo{values: map[string]string{}}
	settings := &SettingService{settingRepo: repo}
	upstream := &policyCaptureUpstream{}
	s := &OpenAIGatewayService{settingService: settings, httpUpstream: upstream}
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	_, err := settings.SetUserOpenAIModelPolicy(ctx, 17, "terra")
	require.NoError(t, err)
	for _, model := range []string{"gpt-6", "gpt-6-astra", "openai/GPT-5.6", "gpt-5.6-sol-high", "gpt-5.6-max", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-4o"} {
		req, _ := http.NewRequestWithContext(ctx, "POST", "https://stub/v1/responses", bytes.NewReader([]byte(`{"model":"`+model+`"}`)))
		req.Header.Set("Content-Type", "application/json")
		_, err = s.doOpenAIUpstream(req, "", account)
		require.NoError(t, err)
		want := "gpt-5.6-terra"
		if model == "gpt-5.6-luna" || model == "gpt-4o" {
			want = model
		}
		require.Equal(t, want, upstream.models[len(upstream.models)-1], model)
	}
	// Same authenticated identity, fresh lookup after policy changes; no auth snapshot.
	_, err = settings.SetUserOpenAIModelPolicy(ctx, 17, "luna")
	require.NoError(t, err)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://stub/v1/chat/completions", bytes.NewReader([]byte(`{"model":"gpt-6"}`)))
	req.Header.Set("Content-Type", "application/json")
	_, err = s.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	require.Equal(t, "gpt-5.6-luna", upstream.models[len(upstream.models)-1])
	count := len(upstream.models)
	repo.err = errors.New("database offline")
	req, _ = http.NewRequestWithContext(ctx, "POST", "https://stub/v1/messages", bytes.NewReader([]byte(`{"model":"gpt-4o"}`)))
	req.Header.Set("Content-Type", "application/json")
	_, err = s.doOpenAIUpstream(req, "", account)
	require.Error(t, err)
	require.Len(t, upstream.models, count)
}

func TestOpenAIUserModelPolicyAdversarialBodies(t *testing.T) {
	settings := &SettingService{settingRepo: &userModelPolicyRepo{values: map[string]string{}}}
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
	_, err := settings.SetUserOpenAIModelPolicy(ctx, 17, "luna")
	require.NoError(t, err)
	s := &OpenAIGatewayService{settingService: settings}
	for _, model := range []string{"openai/GPT-6-ASTRA-2026-09-04", "gpt-6-astra-high", "gpt-5.6-sol-2026-09-04", "gpt-5.6-terra-high", "openai/gpt_5.6_sol"} {
		got, e := s.ClampUserOpenAIModel(ctx, model)
		require.NoError(t, e)
		require.Equal(t, "gpt-5.6-luna", got, model)
	}
	for _, body := range []string{`{"model":"gpt-4o","model":"gpt-6"}`, `{"model":"gpt-4o","Model":"gpt-6"}`, `{"model":17}`, `{"session":{"model":"gpt-4o","model":"gpt-6"}}`} {
		_, e := s.enforceUserOpenAIModelBody(ctx, nil, []byte(body))
		require.Error(t, e, body)
	}
	for _, body := range []string{`{"type":"session.update","session":{"model":"gpt-6-astra"}}`, `{"type":"response.create","response":{"model":"gpt-6-astra"}}`} {
		b, e := s.enforceUserOpenAIModelBody(ctx, nil, []byte(body))
		require.NoError(t, e)
		require.NotContains(t, string(b), "gpt-6-astra")
		require.Contains(t, string(b), "gpt-5.6-luna")
	}
}

func TestOpenAIUserModelPolicyNoHeaderBypass(t *testing.T) {
	repo := &userModelPolicyRepo{values: map[string]string{}}
	settings := NewSettingService(repo, nil)
	_, err := settings.SetUserOpenAIModelPolicy(context.Background(), 17, "luna")
	require.NoError(t, err)
	upstream := &policyCaptureUpstream{}
	s := &OpenAIGatewayService{settingService: settings, httpUpstream: upstream}
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, nil)
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://stub/v1/responses", bytes.NewBufferString(`{"model":"gpt-6-astra"}`))
	_, err = s.doOpenAIUpstream(req, "", &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey})
	require.NoError(t, err)
	require.Equal(t, []string{"gpt-5.6-luna"}, upstream.models)
}

func TestOpenAIUserModelPolicyIngressMapping(t *testing.T) {
	settings := &SettingService{settingRepo: &userModelPolicyRepo{values: map[string]string{}}}
	ch := &ChannelService{}
	ch.cache.Store(populateChannelCache([]Channel{{ID: 1, Status: StatusActive, GroupIDs: []int64{6}, ModelMapping: map[string]map[string]string{PlatformOpenAI: {"gpt-5.6-sol": "gpt-5.6-terra", "codex-auto-review": "gpt-5.6-luna", "gpt-5.6-terra": "gpt-6-astra"}}}}, map[int64]string{6: PlatformOpenAI}))
	s := &OpenAIGatewayService{settingService: settings, channelService: ch}
	g := int64(6)
	ctx := WithOpenAIUserModelPolicy(context.Background(), 17, &g)
	for _, tc := range []struct{ level, model, want string }{
		{"original", "gpt-5.6-sol", "gpt-5.6-terra"},
		{"terra", "openai/GPT-6", "gpt-5.6-terra"},
		{"luna", "gpt-5.6-sol-high", "gpt-5.6-luna"},
		{"full", "gpt-5.6-sol", "gpt-5.6-sol"},
		{"full", "codex-auto-review", "codex-auto-review"},
	} {
		if tc.level != "original" {
			_, err := settings.SetUserOpenAIModelPolicy(ctx, 17, tc.level)
			require.NoError(t, err)
		}
		m, err := s.ResolveUserOpenAIChannelMapping(ctx, &g, tc.model)
		require.NoError(t, err)
		require.Equal(t, tc.want, m.MappedModel, tc)
	}
	// Different user in the same group still sees the shared original mappings.
	other := WithOpenAIUserModelPolicy(context.Background(), 18, &g)
	m, err := s.ResolveUserOpenAIChannelMapping(other, &g, "gpt-5.6-sol")
	require.NoError(t, err)
	require.Equal(t, "gpt-5.6-terra", m.MappedModel)
}

func TestOpenAIUserModelPolicyPersistence(t *testing.T) {
	repo := &userModelPolicyRepo{values: map[string]string{}}
	s := &SettingService{settingRepo: repo}
	ctx := context.Background()
	level, err := s.GetUserOpenAIModelPolicy(ctx, 17)
	require.NoError(t, err)
	require.Equal(t, "original", level)
	level, err = s.SetUserOpenAIModelPolicy(ctx, 17, "terra")
	require.NoError(t, err)
	require.Equal(t, "terra", level)
	level, err = s.GetUserOpenAIModelPolicy(ctx, 18)
	require.NoError(t, err)
	require.Equal(t, "original", level)
	for _, v := range []string{"", "original", "Terra", " terra", "admin"} {
		_, err = s.SetUserOpenAIModelPolicy(ctx, 17, v)
		require.Error(t, err)
	}
	_, err = s.SetUserOpenAIModelPolicy(ctx, 0, "full")
	require.Error(t, err)
}

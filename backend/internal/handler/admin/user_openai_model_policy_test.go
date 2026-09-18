package admin

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"testing"
)

type modelPolicyAdmin struct{ service.AdminService }

func (*modelPolicyAdmin) GetUser(_ context.Context, id int64) (*service.User, error) {
	if id == 99 {
		return nil, service.ErrUserNotFound
	}
	return &service.User{ID: id}, nil
}

type modelPolicySettings struct {
	service.SettingRepository
	values map[string]string
}

func (r *modelPolicySettings) GetValue(_ context.Context, k string) (string, error) {
	v, ok := r.values[k]
	if !ok {
		return "", service.ErrSettingNotFound
	}
	return v, nil
}
func (r *modelPolicySettings) Set(_ context.Context, k, v string) error { r.values[k] = v; return nil }
func TestUserOpenAIModelPolicyAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := &UserHandler{adminService: &modelPolicyAdmin{}, settingService: service.NewSettingService(&modelPolicySettings{values: map[string]string{}}, nil)}
	r.GET("/users/:id/openai-model-policy", h.GetOpenAIModelPolicy)
	r.PUT("/users/:id/openai-model-policy", h.PutOpenAIModelPolicy)
	call := func(method, id, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/users/"+id+"/openai-model-policy", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		return w
	}
	require.JSONEq(t, `{"code":0,"message":"success","data":{"level":"original"}}`, call("GET", "1", "").Body.String())
	for _, level := range []string{"terra", "luna", "full"} {
		w := call("PUT", "1", `{"level":"`+level+`"}`)
		require.Equal(t, 200, w.Code, w.Body.String())
		require.JSONEq(t, `{"code":0,"message":"success","data":{"level":"`+level+`"}}`, call("GET", "1", "").Body.String())
	}
	for _, body := range []string{`{}`, `null`, `{"level":"original"}`, `{"level":"terra","x":1}`, `{"level":"terra"} {}`, `{"level":"terra","level":"full"}`, `{"Level":"terra"}`, `{"level":null}`, `{"level":4}`} {
		require.Equal(t, 400, call("PUT", "1", body).Code, body)
	}
	for _, id := range []string{"0", "-1", "abc", "9223372036854775808"} {
		require.Equal(t, 400, call("GET", id, "").Code)
	}
	require.Equal(t, 404, call("GET", "99", "").Code)
	require.Equal(t, 404, call("PUT", "99", `{"level":"full"}`).Code)
}

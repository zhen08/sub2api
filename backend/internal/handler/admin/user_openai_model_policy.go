package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/gin-gonic/gin"
)

func (h *UserHandler) openAIModelPolicyUser(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid user ID")
		return 0, false
	}
	if _, err = h.adminService.GetUser(c.Request.Context(), id); err != nil {
		response.ErrorFrom(c, err)
		return 0, false
	}
	return id, true
}

// GetOpenAIModelPolicy reads the durable policy, not an authentication snapshot.
func (h *UserHandler) GetOpenAIModelPolicy(c *gin.Context) {
	id, ok := h.openAIModelPolicyUser(c)
	if !ok {
		return
	}
	level, err := h.settingService.GetUserOpenAIModelPolicy(c.Request.Context(), id)
	if err != nil {
		response.Error(c, http.StatusServiceUnavailable, "User model policy unavailable")
		return
	}
	response.Success(c, gin.H{"level": level})
}

func (h *UserHandler) PutOpenAIModelPolicy(c *gin.Context) {
	id, ok := h.openAIModelPolicyUser(c)
	if !ok {
		return
	}
	// Token-level exact shape rejects duplicate/case-folded keys, unknown fields,
	// trailing JSON and null. Keep controller commands unambiguous and bounded.
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 1024))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		response.BadRequest(c, "Expected object with level")
		return
	}
	key, err := decoder.Token()
	if err != nil || key != "level" {
		response.BadRequest(c, "Expected level")
		return
	}
	var level string
	if err = decoder.Decode(&level); err != nil || (level != "terra" && level != "luna" && level != "full") {
		response.BadRequest(c, "level must be terra, luna or full")
		return
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		response.BadRequest(c, "Only one level field is allowed")
		return
	}
	if _, err = decoder.Token(); err != io.EOF {
		response.BadRequest(c, "Unexpected trailing JSON")
		return
	}
	actual, err := h.settingService.SetUserOpenAIModelPolicy(c.Request.Context(), id, level)
	if err != nil {
		response.Error(c, http.StatusServiceUnavailable, "User model policy update could not be verified")
		return
	}
	response.Success(c, gin.H{"level": actual})
}

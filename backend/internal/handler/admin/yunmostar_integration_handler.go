package admin

import (
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type YunMoStarIntegrationHandler struct {
	service *service.YunMoStarIntegrationService
}

func NewYunMoStarIntegrationHandler(service *service.YunMoStarIntegrationService) *YunMoStarIntegrationHandler {
	return &YunMoStarIntegrationHandler{service: service}
}

func (h *YunMoStarIntegrationHandler) CreateRelayKey(c *gin.Context) {
	var input service.YunMoStarRelayKeyInput
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "Invalid request")
		return
	}
	result, err := h.service.Create(c.Request.Context(), input)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *YunMoStarIntegrationHandler) ImportRelayKey(c *gin.Context) {
	var input service.YunMoStarRelayKeyInput
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "Invalid request")
		return
	}
	result, err := h.service.Import(c.Request.Context(), strings.TrimSpace(c.Param("sourceKeyId")), input)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *YunMoStarIntegrationHandler) DeleteRelayKey(c *gin.Context) {
	if err := h.service.Delete(c.Request.Context(), strings.TrimSpace(c.Param("sourceKeyId"))); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"deleted": true})
}

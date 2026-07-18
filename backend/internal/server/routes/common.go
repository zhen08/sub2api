package routes

import (
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
	"github.com/gin-gonic/gin"
)

// RegisterCommonRoutes 注册通用路由（健康检查、状态等）
func RegisterCommonRoutes(r *gin.Engine, auditRuntimes ...*callaudit.Runtime) {
	// 健康检查
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	if len(auditRuntimes) > 0 && auditRuntimes[0] != nil {
		runtime := auditRuntimes[0]
		r.GET("/health/call-audit", func(c *gin.Context) {
			enabled := runtime.Enabled()
			ready := runtime.Ready()
			status := http.StatusOK
			if enabled && !ready {
				status = http.StatusServiceUnavailable
			}
			state := "ok"
			if !enabled {
				state = "disabled"
			} else if !ready {
				state = "degraded"
			}
			// Keep the unauthenticated probe O(1). Backlog/disk scans and detailed
			// failure counters are available only on the authenticated admin route.
			c.JSON(status, gin.H{"status": state, "enabled": enabled, "ready": ready})
		})
	}

	// Claude Code 遥测日志（忽略，直接返回200）
	r.POST("/api/event_logging/batch", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// Setup status endpoint (always returns needs_setup: false in normal mode)
	// This is used by the frontend to detect when the service has restarted after setup
	r.GET("/setup/status", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"needs_setup": false,
				"step":        "completed",
			},
		})
	})
}

// RegisterCallAuditAdminRoutes exposes operational counters only behind the
// existing administrator authentication chain; the public health endpoint is
// intentionally limited to a coarse status.
func RegisterCallAuditAdminRoutes(v1 *gin.RouterGroup, runtime *callaudit.Runtime, adminAuth gin.HandlerFunc) {
	if v1 == nil || runtime == nil || adminAuth == nil {
		return
	}
	group := v1.Group("/admin/call-audit")
	group.Use(adminAuth)
	group.GET("/health", func(c *gin.Context) {
		snapshot := runtime.Snapshot()
		status := http.StatusOK
		if snapshot.Enabled && !snapshot.Ready {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, snapshot)
	})
}

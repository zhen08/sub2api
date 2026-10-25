package service

import (
	"os"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

// A blue/green peer must not delete another live process's Redis request slots.
// Expired slots still age out through their existing TTL/cleanup mechanisms.
func deploymentMultiInstance() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("SUB2API_MULTI_INSTANCE")), "true")
}

// During overlap the old instance owns global periodic jobs. Request-local
// writers, billing, queue handling and cache invalidation must continue running.
// After retirement, a worker-owning instance is started with this flag enabled.
func deploymentBackgroundJobsEnabled() bool {
	return !strings.EqualFold(strings.TrimSpace(os.Getenv("SUB2API_BACKGROUND_JOBS_ENABLED")), "false")
}

func startDeploymentBackgroundJob(name string, start func()) {
	if !deploymentBackgroundJobsEnabled() {
		logger.LegacyPrintf("service.deployment", "[Deployment] periodic job disabled for gateway instance: %s", name)
		return
	}
	start()
}

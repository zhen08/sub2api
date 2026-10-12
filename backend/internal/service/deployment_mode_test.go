//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type deploymentAuditRepository struct {
	AuditLogRepository
	entries []*AuditLog
}

func (r *deploymentAuditRepository) BatchInsert(_ context.Context, entries []*AuditLog) (int64, error) {
	r.entries = append(r.entries, entries...)
	return int64(len(entries)), nil
}

func TestGatewayInstanceStillPersistsRequestAudit(t *testing.T) {
	t.Setenv("SUB2API_BACKGROUND_JOBS_ENABLED", "false")
	repo := &deploymentAuditRepository{}
	svc := NewAuditLogService(repo, nil)
	svc.Start()
	entry := &AuditLog{Action: "bluegreen.audit.probe"}
	svc.Record(entry)
	svc.Stop() // Joins the writer after flushing queued request records.
	require.Equal(t, []*AuditLog{entry}, repo.entries)
}

func TestMultiInstanceStartupPreservesPeerSlots(t *testing.T) {
	t.Setenv("SUB2API_MULTI_INSTANCE", "true")
	cache := &trackingConcurrencyCache{}
	ProvideConcurrencyService(cache, nil, nil)
	require.Empty(t, cache.cleanupPrefix, "starting green must never clear blue's live slots")
	t.Setenv("SUB2API_MULTI_INSTANCE", "false")
	ProvideConcurrencyService(cache, nil, nil)
	require.Equal(t, RequestIDPrefix(), cache.cleanupPrefix, "single-instance startup retains existing behavior")
}

func TestGatewayInstanceDoesNotStartScheduledTests(t *testing.T) {
	t.Setenv("SUB2API_BACKGROUND_JOBS_ENABLED", "false")
	svc := NewScheduledTestRunnerService(nil, nil, nil, nil, nil)
	svc.Start()
	require.Nil(t, svc.cron, "no second scheduler may execute the old instance's plans")
	svc.Stop()
}

func TestGatewayInstanceCannotScheduleMonitorThroughAdminHook(t *testing.T) {
	t.Setenv("SUB2API_BACKGROUND_JOBS_ENABLED", "false")
	runner := NewChannelMonitorRunner(nil, nil)
	runner.Schedule(&ChannelMonitor{ID: 1, Enabled: true})
	require.Empty(t, runner.tasks, "Schedule must obey standby mode as well as Start")
	runner.Stop()
}

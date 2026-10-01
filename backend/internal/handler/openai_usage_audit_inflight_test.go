package handler

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/callaudit"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestUsageTaskCompletesAuditAndInflightReservation(t *testing.T) {
	for _, outcome := range []string{"executed", "dropped", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			spool, err := callaudit.NewSpool(t.TempDir(), 1024)
			require.NoError(t, err)
			scope, err := callaudit.NewScope(callaudit.ScopeInput{RequestID: outcome}, 1, time.Now())
			require.NoError(t, err)
			session, err := callaudit.NewSession(scope, spool, time.Second)
			require.NoError(t, err)

			cache := newHandlerInflightCache(10)
			cfg := &config.Config{}
			cfg.Billing.InflightReservation = config.InflightReservationConfig{Enabled: true, TTLSeconds: 60}
			billing := service.NewBillingCacheService(cache, nil, nil, nil, nil, nil, cfg, nil)
			t.Cleanup(billing.Stop)
			c := newInflightTestGinContext()
			c.Request = c.Request.WithContext(callaudit.WithSession(c.Request.Context(), session))
			handlerDone, err := reserveInflightBalance(c, billing, &countingEstimator{cost: 0.9, priced: true}, &service.APIKey{User: &service.User{ID: 5}}, nil, tokenInflightEstimate("m", nil))
			require.NoError(t, err)
			t.Cleanup(handlerDone)

			ran := false
			task, abandon := wrapUsageRecordTaskContext(c.Request.Context(), func(ctx context.Context) {
				ran = true
				captured, ok := callaudit.SessionFromContext(ctx)
				require.True(t, ok)
				require.Same(t, session, captured)
				if outcome == "panic" {
					panic("billing failed")
				}
			})
			handlerDone()
			require.Equal(t, 1, cache.count(), "billing still owns the reservation")
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			require.False(t, session.WaitPendingUsage(cancelled), "audit must wait for billing")

			switch outcome {
			case "executed":
				task(context.Background())
			case "dropped":
				abandon()
			case "panic":
				require.Panics(t, func() { task(context.Background()) })
			}
			abandon() // Cleanup remains safe after execution or another abandonment.
			require.Equal(t, outcome != "dropped", ran)
			require.Zero(t, cache.count(), "reservation is released on every terminal path")
			require.True(t, session.WaitPendingUsage(cancelled), "audit has no stranded billing work")
		})
	}
}

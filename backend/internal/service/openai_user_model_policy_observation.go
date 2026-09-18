package service

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"sync"
)

type userModelDispatchKey struct{}
type userModelDispatchObservation struct {
	mu        sync.Mutex
	model     string
	rewritten bool
}

// Dispatch observation is per invocation, not a cached authorization decision.
// It survives context detachment and allows final-gate rewrites to update the
// result used by scheduling, usage and billing after protocol conversion.
func beginUserModelDispatch(ctx context.Context, c *gin.Context) (context.Context, func(*OpenAIForwardResult)) {
	observation := &userModelDispatchObservation{}
	ctx = context.WithValue(ctx, userModelDispatchKey{}, observation)
	return ctx, func(result *OpenAIForwardResult) {
		observation.mu.Lock()
		model := observation.model
		rewritten := observation.rewritten
		observation.mu.Unlock()
		if model == "" {
			return
		}
		SetOpsUpstreamModel(c, model)
		if result != nil {
			result.UpstreamModel = model
			if rewritten && result.ImageCount == 0 {
				result.BillingModel = model
			}
		}
	}
}
func observeUserModelDispatch(ctx context.Context, before, after []byte) {
	original := gjson.GetBytes(before, "model").String()
	actual := gjson.GetBytes(after, "model").String()
	if actual == "" {
		return
	}
	if observation, ok := ctx.Value(userModelDispatchKey{}).(*userModelDispatchObservation); ok {
		observation.mu.Lock()
		observation.model = actual
		observation.rewritten = original != actual
		observation.mu.Unlock()
	}
}

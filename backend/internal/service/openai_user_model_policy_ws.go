package service

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"

	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	coderws "github.com/coder/websocket"
)

// Gate belongs to the turn/lease, never to a pooled physical connection.
func (s *OpenAIGatewayService) writeUserPolicyWSJSON(ctx context.Context, lease *openAIWSConnLease, account *Account, value any) error {
	_, _, err := s.writeUserPolicyWSJSONFinalized(ctx, lease, account, value)
	return err
}

func (s *OpenAIGatewayService) writeUserPolicyWSJSONFinalized(ctx context.Context, lease *openAIWSConnLease, account *Account, value any) ([]byte, string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	if _, ok := ctx.Value(openAIUserModelPolicyContextKey{}).(openAIUserModelPolicyIdentity); ok {
		body, err = s.finalizeUserOpenAIModelBody(ctx, account, body)
		if err != nil {
			return nil, "", err
		}
	}
	if err = lease.WriteJSONWithContextTimeout(ctx, json.RawMessage(body), s.openAIWSWriteTimeout()); err != nil {
		return nil, "", err
	}
	return body, strings.TrimSpace(gjson.GetBytes(body, "model").String()), nil
}

// A passthrough relay owns a dedicated upstream connection; each frame is
// nevertheless checked against the live policy using its write context.
type userPolicyWSFrameConn struct {
	openaiwsv2.FrameConn
	service        *OpenAIGatewayService
	account        *Account
	ginContext     *gin.Context
	mu             sync.Mutex
	finishDispatch func(*OpenAIForwardResult)
	dispatchModel  string
}

func (c *userPolicyWSFrameConn) applyDispatch(result *OpenAIForwardResult) {
	c.mu.Lock()
	finish := c.finishDispatch
	c.finishDispatch = nil
	c.dispatchModel = ""
	c.mu.Unlock()
	if finish != nil {
		finish(result)
	}
}

func (c *userPolicyWSFrameConn) currentDispatchModel(fallback string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if model := strings.TrimSpace(c.dispatchModel); model != "" {
		return model
	}
	return strings.TrimSpace(fallback)
}

func (c *userPolicyWSFrameConn) WriteFrame(ctx context.Context, kind coderws.MessageType, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var finish func(*OpenAIForwardResult)
	if gjson.GetBytes(body, "type").String() == "response.create" {
		ctx, finish = beginUserModelDispatch(ctx, c.ginContext)
	}
	var err error
	body, err = c.service.finalizeUserOpenAIModelBody(ctx, c.account, body)
	if err != nil {
		return err
	}
	if err = c.FrameConn.WriteFrame(ctx, kind, body); err != nil {
		return err
	}
	if finish != nil {
		finish(nil)
		c.finishDispatch = finish
		c.dispatchModel = strings.TrimSpace(gjson.GetBytes(body, "model").String())
	}
	return nil
}

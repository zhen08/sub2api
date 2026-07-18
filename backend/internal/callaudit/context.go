package callaudit

import "context"

type sessionContextKey struct{}

func WithSession(ctx context.Context, session *Session) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, sessionContextKey{}, session)
}

func SessionFromContext(ctx context.Context) (*Session, bool) {
	if ctx == nil {
		return nil, false
	}
	session, ok := ctx.Value(sessionContextKey{}).(*Session)
	return session, ok && session != nil
}

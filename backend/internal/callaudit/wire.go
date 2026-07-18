package callaudit

import "github.com/google/wire"

var ProviderSet = wire.NewSet(NewRuntime)

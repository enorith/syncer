package syncer

import (
	"math"
	"strconv"
	"strings"
	"sync"
)

type Resolver func(value interface{}, item map[string]interface{}, args ...string) interface{}

var (
	valueResolvers = make(map[string]Resolver)
	resolverMu     sync.RWMutex
)

func RegisterValueResolver(name string, resolver Resolver) {
	resolverMu.Lock()
	defer resolverMu.Unlock()
	valueResolvers[name] = resolver
}

func ResolveValue(value interface{}, item map[string]interface{}, resolver string, args ...string) interface{} {
	if r, ok := valueResolvers[resolver]; ok {
		return r(value, item, args...)
	}

	return value
}

func trimResolver(value interface{}, item map[string]interface{}, args ...string) interface{} {
	if str, ok := value.(string); ok {
		return strings.TrimSpace(str)
	}

	return value
}

func intResolver(value interface{}, item map[string]interface{}, args ...string) interface{} {
	if str, ok := value.(string); ok {
		i, _ := strconv.ParseInt(str, 10, 64)

		return i
	}

	if fl, ok := value.(float64); ok {
		return int64(math.Floor(fl))
	}

	return value
}

func init() {
	RegisterValueResolver("trim", trimResolver)
	RegisterValueResolver("int", intResolver)
}

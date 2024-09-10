package ds

import (
	"fmt"
	"net/url"
	"sync"
)

type ListMeta struct {
	Total int64 `json:"total"`
}

type ListFilter struct {
	Field string
	Op    string
	Value interface{}
}

type ListOrder struct {
	Field string
	Order string
}

type ListOption struct {
	Limit       int64
	Page        int64
	WithoutMeta bool

	Selects []string
	Filters []ListFilter
	Orders  []ListOrder
}

type ListResult struct {
	Meta ListMeta `json:"meta"`
	Data []any    `json:"data"`
}

type Datasource interface {
	List(opt ListOption) (ListResult, error)
	Find(id any) (any, error)
	Create(data any) error
	Update(id any, data any) error
	UpdateMany(data any, filters ...ListFilter) error
	Delete(id any) error
	DeleteMany(filters ...ListFilter) error

	ListMeta(filters ...ListFilter) (ListMeta, error)
}

type Register func(u *url.URL) (Datasource, error)

var (
	registers = make(map[string]Register)
	mu        = new(sync.RWMutex)
)

func RegisterDatasource(schema string, register Register) {
	mu.Lock()
	defer mu.Unlock()
	registers[schema] = register
}

func GetRegister(schema string) (Register, bool) {
	mu.RLock()
	defer mu.RUnlock()
	register, ok := registers[schema]

	return register, ok
}

func Connect(connUrl string) (Datasource, error) {
	u, e := url.Parse(connUrl)

	if e != nil {
		return nil, e
	}

	register, ok := GetRegister(u.Scheme)
	if !ok {
		return nil, fmt.Errorf("unregistered datasource: %s", u.Scheme)
	}

	return register(u)
}

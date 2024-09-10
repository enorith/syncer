package ds

import (
	"errors"
	"net/url"
	"reflect"
	"strings"
	"sync"

	"github.com/enorith/gormdb"
	"github.com/enorith/supports/collection"
	"gorm.io/gorm"
)

type DBListModel interface {
	AggTableScope() func(*gorm.DB) *gorm.DB
	ListScope() func(*gorm.DB) *gorm.DB
}

type DB struct {
	tx        *gorm.DB
	table, pk string
	model     any
}

func (db *DB) List(opt ListOption) (ListResult, error) {
	tx := db.newSession()
	newTx := db.newSession()
	model := db.newModel()
	m, isModel := model.(DBListModel)

	tx = tx.Model(model).Table(db.table).Scopes(func(d *gorm.DB) *gorm.DB {
		for _, filter := range opt.Filters {
			d = db.applyFilter(d, filter)
		}

		if len(opt.Selects) > 0 {
			d = d.Select(opt.Selects)
		}

		return d
	})

	if isModel {
		tx = tx.Scopes(m.ListScope())
	}

	result := ListResult{
		Data: make([]any, 0),
		Meta: ListMeta{
			Total: 0,
		},
	}

	if !opt.WithoutMeta {
		aggTable := tx.Session(&gorm.Session{})
		if isModel {
			aggTable = aggTable.Scopes(m.AggTableScope())
		}

		e := newTx.Table("(?) aggragate", aggTable).Count(&result.Meta.Total).Error
		if e != nil {
			return result, e
		}
	}

	if len(opt.Orders) > 0 {
		orders := collection.Map(opt.Orders, func(order ListOrder) string {
			return order.Field + " " + order.Order
		})

		tx = tx.Order(strings.Join(orders, ", "))
	}
	if opt.Limit > 0 {
		tx = tx.Limit(int(opt.Limit))
	}

	if opt.Page < 1 {
		opt.Page = 1
	}

	tx = tx.Offset((int((opt.Page - 1) * opt.Limit)))
	var e error
	if _, ok := db.model.(MapModel); ok {
		var sv []map[string]any

		e = tx.Find(&sv).Error

		if e == nil {
			for _, v := range sv {
				result.Data = append(result.Data, v)
			}
		}
	} else {
		sv := db.newModelSlice()
		e = tx.Find(&sv).Error

		if e == nil {
			vdv := reflect.ValueOf(sv)
			for i := 0; i < vdv.Len(); i++ {
				result.Data = append(result.Data, vdv.Index(i).Interface())
			}
		}
	}

	return result, e

}

func (db *DB) ListMeta(filters ...ListFilter) (ListMeta, error) {
	tx := db.newSession()
	newTx := db.newSession()
	model := db.newModel()
	tx = tx.Model(model).Table(db.table).Scopes(func(d *gorm.DB) *gorm.DB {
		for _, filter := range filters {
			d = db.applyFilter(d, filter)
		}

		return d
	})

	m, isModel := model.(DBListModel)
	aggTable := tx.Session(&gorm.Session{})
	if isModel {
		aggTable = aggTable.Scopes(m.AggTableScope())
	}

	meta := ListMeta{
		Total: 0,
	}

	e := newTx.Table("(?) aggragate", aggTable).Count(&meta.Total).Error

	return meta, e
}

func (db *DB) Find(id any) (any, error) {
	model := db.newModel()

	e := db.newSession().Table(db.table).Find(model, db.pk+" = ?", id).Error

	return model, e
}

func (db *DB) Create(data any) error {
	return db.newSession().Table(db.table).Create(data).Error
}

func (db *DB) Update(id any, data any) error {
	return db.newSession().Table(db.table).Where(db.pk+" = ?", id).Updates(data).Error
}

func (db *DB) UpdateMany(data any, filters ...ListFilter) error {
	tx := db.newSession().Table(db.table)
	for _, filter := range filters {
		tx = db.applyFilter(tx, filter)
	}
	return tx.Updates(data).Error
}

func (db *DB) Delete(id any) error {
	model := db.newModel()
	return db.newSession().Table(db.table).Where(db.pk+" = ?", id).Delete(model).Error
}

func (db *DB) DeleteMany(filters ...ListFilter) error {
	model := db.newModel()
	tx := db.newSession().Table(db.table)

	for _, filter := range filters {
		tx = db.applyFilter(tx, filter)
	}

	return tx.Delete(model).Error
}

func (db *DB) applyFilter(tx *gorm.DB, filter ListFilter) *gorm.DB {
	switch strings.ToLower(filter.Op) {
	case "between":
		if v, ok := filter.Value.([]any); ok {
			if len(v) == 2 {
				tx = tx.Where(filter.Field+" BETWEEN ? AND ?", v[0], v[1])
			}
		}
	default:
		tx = tx.Where(filter.Field+" "+filter.Op+" ?", filter.Value)
	}

	return tx
}

func (db *DB) newSession() *gorm.DB {
	return db.tx.Session(&gorm.Session{NewDB: true})
}

func (db *DB) newModel() any {
	return reflect.New(reflect.TypeOf(db.model)).Interface()
}

func (db *DB) newModelSlice() any {
	t := reflect.TypeOf(db.model)
	return reflect.MakeSlice(reflect.SliceOf(t), 0, 0).Interface()
}

type NewDBConfig struct {
	Table, PK string
	Model     any
}

func NewDB(tx *gorm.DB, conf NewDBConfig) *DB {
	return &DB{tx: tx, table: conf.Table, pk: conf.PK, model: conf.Model}
}

var (
	dbModels = make(map[string]any)
	dbLock   = new(sync.RWMutex)
)

func RegisterDBModel(name string, model any) {
	dbLock.Lock()
	defer dbLock.Unlock()
	dbModels[name] = model
}

func GetDBModel(name string) (any, bool) {
	dbLock.RLock()
	defer dbLock.RUnlock()
	model, ok := dbModels[name]
	return model, ok
}

func DBRegister(url *url.URL) (Datasource, error) {
	conn := url.Host

	db, e := gormdb.DefaultManager.GetConnection(conn)
	if e != nil {
		return nil, e
	}

	table := url.Path[1:]

	if table == "" {
		return nil, errors.New("[datasource] db table is required")
	}

	m := url.Query().Get("model")

	model, ok := GetDBModel(m)

	if !ok {
		model, ok = GetDBModel(table)
	}

	if !ok {
		model = MapModel(table)
	}

	pk := url.Query().Get("pk")

	if pk == "" {
		pk = "id"
	}

	return &DB{tx: db, table: table, pk: pk, model: model}, nil
}

type MapModel string

func (m MapModel) TableName() string {
	return string(m)
}

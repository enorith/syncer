package syncer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/enorith/supports/collection"
	"github.com/enorith/supports/dbutil"
	"github.com/enorith/syncer/ds"
	jsoniter "github.com/json-iterator/go"
	"gorm.io/gorm"
)

var DefaultTimeFormat = "2006-01-02 15:04:05"

type TargetConfig struct {
	rawData []byte
}

func (t TargetConfig) RawData() []byte {
	return t.rawData
}

func (t TargetConfig) Unmarshal(val interface{}) error {
	return jsoniter.Unmarshal(t.rawData, val)
}

func (t *TargetConfig) UnmarshalJSON(row []byte) error {
	t.rawData = row
	return nil
}

type Target interface {
	SyncFrom(conf TargetConfig, data []map[string]any, meta *SyncMeta) error
	BeforeSync(conf TargetConfig, meta *SyncMeta) error
	AfterSync(conf TargetConfig, meta *SyncMeta) error
}

var (
	SyncerTargets = make(map[string]Target)
	mu            = new(sync.RWMutex)
)

func RegisterTarget(name string, target Target) {
	mu.Lock()
	defer mu.Unlock()
	SyncerTargets[name] = target
}

func GetTarget(name string) (Target, bool) {
	mu.RLock()
	defer mu.RUnlock()
	target, ok := SyncerTargets[name]
	return target, ok
}

type DBTargetConfig struct {
	Table   string   `json:"table"`
	Uniques []string `json:"uniques"`
	Updates []string `json:"updates"`

	VersionField    string `json:"version_field"`
	SyncTimeField   string `json:"sync_time_field"`
	SyncTimeFmt     string `json:"sync_time_fmt"`
	SyncStatusField string `json:"sync_status_field"`
	MaxVersion      int    `json:"max_version"`
}

type DBTarget struct {
	db *gorm.DB
}

func (db *DBTarget) SyncFrom(conf TargetConfig, data []map[string]any, meta *SyncMeta) error {
	var config DBTargetConfig
	conf.Unmarshal(&config)

	now := time.Now()

	if config.Table == "" {
		return errors.New("[target] db table is required")
	}

	var opts []dbutil.UpsertOpt
	if len(config.Uniques) > 0 {
		opts = append(opts, dbutil.UpsertOptColumns(config.Uniques...))
	}

	if len(config.Updates) > 0 {
		opts = append(opts, dbutil.UpsertOptUpdateColumns(config.Updates...))
	}

	timeFmt := DefaultTimeFormat

	if config.SyncTimeFmt != "" {
		timeFmt = config.SyncTimeFmt
	}

	tx := db.newSession()

	if config.VersionField != "" || config.SyncTimeField != "" {
		data = collection.Map(data, func(row map[string]any) map[string]any {
			if config.VersionField != "" {
				row[config.VersionField] = meta.Version
			}
			if config.SyncTimeField != "" {
				row[config.SyncTimeField] = now.Format(timeFmt)
			}
			if config.SyncStatusField != "" {
				row[config.SyncStatusField] = 1
			}
			return row
		})
	}

	return tx.Table(config.Table).Scopes(dbutil.WithUpsert(opts...)).Create(data).Error
}

func (db *DBTarget) BeforeSync(conf TargetConfig, meta *SyncMeta) error {
	var config DBTargetConfig
	conf.Unmarshal(&config)

	if config.VersionField != "" {
		tx := db.newSession()

		var version int
		model := ds.MapModel(config.Table)
		e := tx.Model(&model).Select(fmt.Sprintf("MAX(%s)", config.VersionField)).Table(config.Table).Scan(&version).Error

		meta.Version = version + 1

		return e
	}

	return nil
}

func (db *DBTarget) AfterSync(conf TargetConfig, meta *SyncMeta) error {
	var config DBTargetConfig
	conf.Unmarshal(&config)

	if config.SyncStatusField != "" && config.VersionField != "" {
		tx := db.newSession()
		model := ds.MapModel(config.Table)
		e := tx.Model(&model).Table(config.Table).Where(fmt.Sprintf("%s < ?", config.VersionField), meta.Version).Update(config.SyncStatusField, 0).Error

		if e != nil {
			return e
		}
	}

	if config.VersionField != "" && config.MaxVersion > 0 {
		tx := db.newSession()

		model := ds.MapModel(config.Table)

		e := tx.Where(fmt.Sprintf("%s < ? AND %s > ?", config.VersionField, config.VersionField), meta.Version-config.MaxVersion, 0).Table(config.Table).Delete(&model).Error
		if e != nil {
			return e
		}
	}

	return nil
}

func (db *DBTarget) newSession() *gorm.DB {
	return db.db.Session(&gorm.Session{NewDB: true})
}

func NewDBTarget(db *gorm.DB) *DBTarget {
	return &DBTarget{db: db}
}

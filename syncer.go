package syncer

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alitto/pond"
	"github.com/enorith/syncer/ds"
	"github.com/go-co-op/gocron"
)

type SyncerTask struct {
	ID      string            `json:"id"`
	Source  string            `json:"source"`
	Mapping map[string]string `json:"mapping"`
	Filters []ds.ListFilter   `json:"filters"`
	Orders  []ds.ListOrder    `json:"orders"`

	Target       string       `json:"target"`
	TargetConfig TargetConfig `json:"target_config"`

	Size        int64 `json:"size"`
	Workers     int   `json:"workers"`
	StopOnError bool  `json:"stop_on_error"`

	// At 每天的时间，interval 为nd时有效
	At          string `json:"at"`
	Immediately bool   `json:"immediately"`
	Interval    string `json:"interval"`
}

const (
	SyncStatusPending = iota + 1
	SyncStatusRunning
	SyncStatusSuccess
	SyncStatusFailed
)

type SyncMeta struct {
	Version int
	Total   int64
	Status  int
	Error   error
}

type Syncer struct {
	tasks map[string]SyncerTask
	mu    sync.RWMutex
}

func (s *Syncer) AddTask(tasks ...SyncerTask) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, task := range tasks {
		s.tasks[task.ID] = task
	}
}

func (s *Syncer) GetTask(id string) (SyncerTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, ok := s.tasks[id]

	return task, ok
}

func (s *Syncer) Schedule(sch *gocron.Scheduler) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, task := range s.tasks {
		if task.Interval != "" {
			if isD, sub := EndWith(task.Interval, "d", "day", "days"); isD {
				i := task.Interval[:len(task.Interval)-len(sub)]

				ev, e := strconv.Atoi(i)
				if e == nil {
					sch.Every(ev).Day().Tag("syncer:" + task.ID).Do(func() {
						s.SyncTask(task)
					})
				}
			} else {
				sch.Every(task.Interval).Tag("syncer:" + task.ID).Do(func() {
					s.SyncTask(task)
				})
			}
		}
	}
}

func (s *Syncer) DoSync(id string) (int64, error) {
	task, ok := s.GetTask(id)

	if !ok {
		return 0, fmt.Errorf("[syncer] task not found: %s", id)
	}

	return s.SyncTask(task)
}

func (s *Syncer) SyncTask(task SyncerTask) (int64, error) {
	dataSource, e := ds.Connect(task.Source)
	if e != nil {
		return 0, e
	}

	meta, e := dataSource.ListMeta(task.Filters...)

	if e != nil {
		return 0, e
	}

	if meta.Total == 0 {
		return 0, nil
	}

	pool := pond.New(task.Workers, 1000)

	maxPage := int(math.Ceil(float64(meta.Total) / float64(task.Size)))

	target, ok := GetTarget(task.Target)

	if !ok {
		return 0, fmt.Errorf("[syncer] target not found: %s", task.Target)
	}

	var syncMeta = SyncMeta{
		Total:  meta.Total,
		Status: SyncStatusPending,
	}

	e = target.BeforeSync(task.TargetConfig, &syncMeta)
	if e != nil {
		return meta.Total, e
	}

	var syncFunc = func(page int64) error {
		data, e := dataSource.List(ds.ListOption{
			Page:        int64(page),
			WithoutMeta: true,
			Filters:     task.Filters,
			Orders:      task.Orders,
			Limit:       task.Size,
		})

		if e != nil {
			return e
		}

		if len(data.Data) == 0 {
			return nil
		}

		var syncData []map[string]any
		for _, dsItem := range data.Data {
			if m, ok := dsItem.(map[string]any); ok {
				item := make(map[string]any)
				for k, v := range task.Mapping {
					parts := strings.Split(v, ";")
					value := m[k]
					for _, part := range parts {
						resolvers := strings.Split(part, "|")
						pk := resolvers[0]
						if len(resolvers) > 1 {
							for _, resolver := range resolvers[1:] {
								partsParams := strings.Split(resolver, ":")
								var args []string
								if len(partsParams) > 1 {
									paramStr := partsParams[1]
									args = strings.Split(paramStr, ",")
									resolver = partsParams[0]
								}
								value = ResolveValue(value, m, resolver, args...)
							}
						}
						item[pk] = value
					}
				}
				syncData = append(syncData, item)
			}
		}

		delayRand := time.Duration(10+rand.Intn(20)) * time.Millisecond

		<-time.After(delayRand)
		return target.SyncFrom(task.TargetConfig, syncData, &syncMeta)
	}

	var (
		errChan = make(chan error)
		exit    = make(chan struct{})
	)

	for page := 1; page <= maxPage; page++ {
		p := page
		pool.Submit(func() {
			e := syncFunc(int64(p))
			if e != nil && task.StopOnError {
				errChan <- e
			}
		})
	}

	go func() {
		pool.StopAndWait()
		exit <- struct{}{}
	}()

	select {
	case <-exit:
		syncMeta.Status = SyncStatusSuccess
	case err := <-errChan:
		syncMeta.Status = SyncStatusFailed
		syncMeta.Error = err
	}

	e = target.AfterSync(task.TargetConfig, &syncMeta)
	if e != nil {
		return meta.Total, e
	}

	return meta.Total, syncMeta.Error
}

func NewSyncer() *Syncer {
	return &Syncer{
		tasks: make(map[string]SyncerTask),
		mu:    sync.RWMutex{},
	}
}

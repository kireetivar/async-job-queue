package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/kireetivar/async-job-queue/models"
)

type HandleFunc func(ctx context.Context, job *models.Job) error

type HandleRegistry struct {
	rw sync.RWMutex
	mp map[string]HandleFunc
}

func (hr *HandleRegistry) Register(jobType string, fn HandleFunc) error {
	if fn == nil {
		return fmt.Errorf("handler for %s cannot be nil", jobType)
	}
	if jobType == "" {
		return fmt.Errorf("jobType cannot be nil while registering")
	}
	hr.rw.Lock()
	defer hr.rw.Unlock()
	if _, ok := hr.mp[jobType]; ok {
		return fmt.Errorf("jobType %s is already registered", jobType)
	}
	hr.mp[jobType] = fn
	return nil
}

func (hr *HandleRegistry) Get(jobType string) (HandleFunc, error) {
	if jobType == "" {
		return nil, fmt.Errorf("value of jobType cant be empty")
	}
	hr.rw.RLock()
	defer hr.rw.RUnlock()
	fn, ok := hr.mp[jobType]
	if !ok {
		return nil, fmt.Errorf("jobTpye : %s not registered", jobType)
	}
	return fn, nil
}

func NewHandlerRegistry() *HandleRegistry {
	return &HandleRegistry{
		mp: make(map[string]HandleFunc),
	}
}

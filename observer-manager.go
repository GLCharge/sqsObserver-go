package sqsObserver_go

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
)

var (
	manager ObserverManager
	once    = sync.Once{}
)

type (
	ObserverManager interface {
		GetDefaultObserver() MultipleObserver
		SetDefaultObserver(observer MultipleObserver)
		GetDefaultPublisher() Publisher
		SetDefaultPublisher(publisher Publisher)
		AddObserver(tag string, observer Observer)
		HasObserverWithTag(tag string) bool
		GetObservers() []Observer
		GetObserver(tag string) Observer
		GetMultipleObserver(tag string) MultipleObserver
		StartObservers(ctx context.Context)
		PublisherListen(ctx context.Context)
	}

	Manager struct {
		mu               sync.Mutex
		once             sync.Once
		defaultObserver  MultipleObserver
		defaultPublisher Publisher
		observers        sync.Map
		tags             sync.Map
	}
)

func createManager() {
	once.Do(func() {
		manager = &Manager{
			mu:               sync.Mutex{},
			once:             sync.Once{},
			observers:        sync.Map{},
			defaultObserver:  nil,
			defaultPublisher: nil,
			tags:             sync.Map{},
		}
	})
}

func GetObserverManager() ObserverManager {
	if manager == nil {
		createManager()
	}

	return manager
}

func (qs *Manager) SetDefaultObserver(observer MultipleObserver) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.defaultObserver = observer
}

func (qs *Manager) SetDefaultPublisher(publisher Publisher) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.defaultPublisher = publisher
}

func (qs *Manager) GetDefaultObserver() MultipleObserver {
	return qs.defaultObserver
}

func (qs *Manager) GetDefaultPublisher() Publisher {
	return qs.defaultPublisher
}

func (qs *Manager) AddObserver(tag string, observer Observer) {
	if observer != nil {

		log.Tracef("Added observer with tag %s", tag)
		// Add to list of non-duplicate tags
		qs.tags.Store(tag, 1)
		qs.observers.Store(tag, observer)
	}
}

func (qs *Manager) StartObservers(ctx context.Context) {
	log.Info("Starting observers")
	// Start the observers only once
	qs.once.Do(func() {
		if qs.defaultObserver != nil {
			go qs.defaultObserver.Start(ctx)
		}

		// For each tag, get the associated observer and start it in a goroutine
		qs.tags.Range(func(key, value interface{}) bool {

			queue, isFound := qs.observers.Load(key)
			if isFound && queue != nil {
				go queue.(Observer).Start(ctx)
				log.Tracef("Started observer with tag %s", key)
			}

			return true
		})
	})
}

func (qs *Manager) PublisherListen(ctx context.Context) {
	if qs.defaultPublisher != nil {
		go qs.defaultPublisher.Listen(ctx)
	}
}

func (qs *Manager) HasObserverWithTag(tag string) bool {
	_, isFound := qs.observers.Load(tag)
	return isFound
}

func (qs *Manager) GetObservers() []Observer {
	var observers []Observer

	qs.tags.Range(func(key, value interface{}) bool {
		observers = append(observers, qs.GetObserver(key.(string)))
		return true
	})

	return observers
}

func (qs *Manager) GetMultipleObserver(tag string) MultipleObserver {
	queue, isFound := qs.observers.Load(tag)
	if isFound {
		switch queue.(type) {
		case MultipleObserver:
			return queue.(MultipleObserver)
		default:
			return nil
		}
	}

	return qs.defaultObserver
}

func (qs *Manager) GetObserver(tag string) Observer {
	queue, isFound := qs.observers.Load(tag)
	if isFound {
		switch queue.(type) {
		case SingleObserver:
			return queue.(SingleObserver)
		case MultipleObserver:
			return queue.(MultipleObserver)
		default:
			return queue.(Observer)
		}
	}

	return qs.defaultObserver
}

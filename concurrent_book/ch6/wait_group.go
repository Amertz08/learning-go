package main

import "sync"

// Semaphore was copy pasta from Ch5 implementation
type Semaphore struct {
	permits int
	cond    *sync.Cond
}

func NewSemaphore(permits int) *Semaphore {
	return &Semaphore{
		permits: permits,
		cond:    sync.NewCond(&sync.Mutex{}),
	}
}

func (s *Semaphore) Acquire() {
	s.cond.L.Lock()
	for s.permits <= 0 {
		s.cond.Wait()
	}
	s.permits--
	s.cond.L.Unlock()
}

func (s *Semaphore) Release() {
	s.cond.L.Lock()
	s.permits++
	s.cond.Signal()
	s.cond.L.Unlock()
}

// WaitGroup implements on top of a Semaphore
type WaitGroup struct {
	sema *Semaphore
}

func NewWaitGroup(size int) *WaitGroup {
	return &WaitGroup{
		sema: NewSemaphore(1 - size),
	}
}

func (wg *WaitGroup) Wait() {
	wg.sema.Acquire()
}

func (wg *WaitGroup) Done() {
	wg.sema.Release()
}

// WaitGrp allows you to add to the pool size on the fly. This is basically the
// baked in version of a WaitGroup
type WaitGrp struct {
	size int
	cond *sync.Cond
}

func NewWaitGrp() *WaitGrp {
	return &WaitGrp{
		size: 0,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (wg *WaitGrp) Add(delta int) {
	wg.cond.L.Lock()
	wg.size += delta
	wg.cond.L.Unlock()
}

func (wg *WaitGrp) Wait() {
	wg.cond.L.Lock()
	for wg.size > 0 {
		wg.cond.Wait()
	}
	wg.cond.L.Unlock()
}

func (wg *WaitGrp) Done() {
	wg.cond.L.Lock()
	wg.size--
	if wg.size == 0 {
		wg.cond.Broadcast()
	}
	wg.cond.L.Unlock()
}

func (wg *WaitGrp) TryLock() bool {
	wg.cond.L.Lock()
	if wg.size == 0 {
		wg.cond.L.Unlock()
		return true
	}
	return false
}

func (wg *WaitGrp) TryWait() bool {
	wg.cond.L.Lock()
	if wg.size > 0 {
		wg.cond.L.Unlock()
		return false
	}
	return true
}

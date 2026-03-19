package ch5

import "sync"

type ReadWriteMutex struct {
	readersCounter int
	writersWaiting int
	writerActive   bool
	cond           *sync.Cond
}

func NewReadWriteMutex() *ReadWriteMutex {
	return &ReadWriteMutex{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (rwm *ReadWriteMutex) ReadLock() {
	rwm.cond.L.Lock()
	for rwm.writersWaiting > 0 || rwm.writerActive {
		rwm.cond.Wait()
	}
	rwm.readersCounter++
	rwm.cond.L.Unlock()
}

func (rwm *ReadWriteMutex) WriteLock() {
	rwm.cond.L.Lock()

	rwm.writersWaiting++
	for rwm.readersCounter > 0 || rwm.writerActive {
		rwm.cond.Wait()
	}

	rwm.writersWaiting--
	rwm.writerActive = true
	rwm.cond.L.Unlock()
}

func (rwm *ReadWriteMutex) ReadUnlock() {
	rwm.cond.L.Lock()
	rwm.readersCounter--
	if rwm.readersCounter == 0 {
		rwm.cond.Broadcast()
	}
	rwm.cond.L.Unlock()
}

func (rwm *ReadWriteMutex) WriteUnlock() {
	rwm.cond.L.Lock()
	rwm.writerActive = false
	rwm.cond.Broadcast()
}

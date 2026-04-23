package ch7

import (
	"container/list"
	"sync"
)

type Channel[M any] struct {
	capacitySema *Semaphore
	sizeSema     *Semaphore
	mutex        sync.Mutex
	buffer       *list.List
}

func NewChannel[M any](capacity int) *Channel[M] {
	return &Channel[M]{
		capacitySema: NewSemaphore(capacity),
		sizeSema:     NewSemaphore(0),
		buffer:       list.New(),
	}
}

func (c *Channel[M]) Send(message M) {
	// Will block until there is a slot in the buffer
	c.capacitySema.Acquire()

	c.mutex.Lock()
	c.buffer.PushBack(message)
	c.mutex.Unlock()

	c.sizeSema.Release()
}

func (c *Channel[M]) Receive() M {
	c.capacitySema.Release()

	// Will block until there is a message in the buffer
	c.sizeSema.Acquire()

	c.mutex.Lock()
	v := c.buffer.Remove(c.buffer.Front()).(M)
	c.mutex.Unlock()

	return v
}

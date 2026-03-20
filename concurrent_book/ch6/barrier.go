package main

import (
	"fmt"
	"sync"
	"time"
)

type Barrier struct {
	size      int
	waitCount int
	cond      *sync.Cond
}

func NewBarrier(size int) *Barrier {
	return &Barrier{
		size:      size,
		waitCount: 0,
		cond:      sync.NewCond(&sync.Mutex{}),
	}
}

func (b *Barrier) Wait() {
	b.cond.L.Lock()
	b.waitCount++
	if b.waitCount == b.size {
		b.waitCount = 0
		b.cond.Broadcast()
	} else {
		b.cond.Wait()
	}
	b.cond.L.Unlock()
}

func workAndWait(name string, timeToWork int, barr *Barrier) {
	start := time.Now()
	for {
		fmt.Println(time.Since(start).String(), name, "working")
		time.Sleep(time.Duration(timeToWork) * time.Second)
		fmt.Println(time.Since(start).String(), name, "waiting on barrier")
		barr.Wait()
	}
}

func main() {
	barrier := NewBarrier(2)
	go workAndWait("A", 1, barrier)
	go workAndWait("B", 5, barrier)
	time.Sleep(20 * time.Second)
}

package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

func main() {
	money := 100
	mutext := sync.Mutex{}
	cond := sync.NewCond(&mutext)

	go stingy(&money, cond)
	go spendy(&money, cond)
	time.Sleep(20 * time.Second)
	mutext.Lock()
	fmt.Println("Money:", money)
	mutext.Unlock()
}

func stingy(money *int, cond *sync.Cond) {
	for i := 0; i < 1_000_000; i++ {
		cond.L.Lock()
		*money += 10
		if *money >= 50 {
			cond.Signal()
		}
		cond.L.Unlock()
	}
	fmt.Println("Stingy")
}

func spendy(money *int, cond *sync.Cond) {
	for i := 0; i < 2_000_000; i++ {
		cond.L.Lock()
		for *money < 50 {
			cond.Wait()
		}

		*money -= 50
		if *money < 0 {
			fmt.Println("Money is negative")
			os.Exit(1)
		}
		cond.L.Unlock()
	}
	fmt.Println("Spendy")
}

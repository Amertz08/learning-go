package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
)

func main() {
	players := []*Player{
		{"bryan", 0, sync.Mutex{}},
		{"adam", 0, sync.Mutex{}},
		{"steve", 0, sync.Mutex{}},
		{"daniel", 0, sync.Mutex{}},
		{"michael", 0, sync.Mutex{}},
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		n := rand.Intn(len(players)) + 1
		rand.Shuffle(len(players), func(i, j int) { players[i], players[j] = players[j], players[i] })
		wg.Add(1)
		sublist := make([]*Player, n)
		copy(sublist, players[:n])
		go func(players []*Player) {
			incrementScores(players, 10)
			wg.Done()
		}(sublist)
	}
	wg.Wait()
	for _, player := range players {
		fmt.Printf("Score for %s is %d\n", player.name, player.score)
	}
}

type Player struct {
	name  string
	score int
	mutex sync.Mutex
}

// change this such that it avoids a deadlock
func incrementScores(players []*Player, increment int) {
	// just sort it
	sort.Slice(players, func(i, j int) bool {
		return players[i].name < players[j].name
	})
	for _, player := range players {
		player.mutex.Lock()
	}
	for _, player := range players {
		player.score += increment
	}
	for _, player := range players {
		player.mutex.Unlock()
	}
}

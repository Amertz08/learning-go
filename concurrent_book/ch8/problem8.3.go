package main

import (
	"math/rand"
	"time"
)

func main() {
	// In the book solution they used an array for the player channels
	player1 := player()
	player2 := player()
	player3 := player()
	player4 := player()

	playerCount := 4

	for playerCount > 1 {
		select {
		case move, open := <-player1:
			// In the book solution they refactored this logic into a function
			if !open {
				// This example problem shows how you can use a nil channel to remove the case from being evaluated
				// by the select statement.
				player1 = nil
				playerCount--
				println("Player 1 closed, player count:", playerCount)
				continue
			}
			println("Player 1 moved:", move)
		case move, open := <-player2:
			if !open {
				player2 = nil
				playerCount--
				println("Player 2 closed, player count:", playerCount)
				continue
			}
			println("Player 2 moved:", move)
		case move, open := <-player3:
			if !open {
				player3 = nil
				playerCount--
				println("Player 3 closed, player count:", playerCount)
				continue
			}
			println("Player 3 moved:", move)
		case move, open := <-player4:
			if !open {
				player4 = nil
				playerCount--
				println("Player 4 closed, player count:", playerCount)
				continue
			}
			println("Player 4 moved:", move)
		}
	}

}

func player() chan string {
	output := make(chan string)
	count := rand.Intn(100)
	move := []string{"UP", "DOWN", "LEFT", "RIGHT"}
	go func() {
		defer close(output)

		for i := 0; i < count; i++ {
			output <- move[rand.Intn(4)]
			d := time.Duration(rand.Intn(200))
			time.Sleep(d * time.Millisecond)
		}
	}()

	return output
}

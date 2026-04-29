package main

import (
	"fmt"
)

// Write a function that reverses the words in a string list
func main() {
	vals := []string{"a", "p", "p", "l", "e", " ", "h", "e", "l", "l", "o"}

	fmt.Println(reverseWordList(vals))

	vals = []string{"a", " ", " ", "b"}
	fmt.Println(reverseWordList(vals))

	vals = []string{"a", " ", "c", " ", " ", "b"}
	fmt.Println(reverseWordList(vals))

	vals = []string{"a", "p", "p", "l", "e", " ", "h", "e", "l", "l", "o"}
	fmt.Println(reverseWordsWithStack(vals))
}

func reverseWordList(vals []string) []string {
	// TODO: not very clean. I feel like this could be refactored
	firstPtr := 0
	for i, ch := range vals {
		// once we see a space, reverse the word
		if ch == " " {
			// We know the word ends at i-1
			endPtr := i - 1
			// While we haven't reached the middle of the word, swap characters
			for firstPtr < endPtr {
				tempVal := vals[firstPtr]
				vals[firstPtr] = vals[endPtr]
				vals[endPtr] = tempVal
				firstPtr++
				endPtr--
			}
			// reset the first pointer to the start of the next word
			// TODO: what if the next char is also a space? it doesn't seem to matter but I don't think this is great.
			firstPtr = i + 1
		} else if ch == vals[len(vals)-1] {
			endPtr := len(vals) - 1
			for firstPtr < endPtr {
				tempVal := vals[firstPtr]
				vals[firstPtr] = vals[endPtr]
				vals[endPtr] = tempVal
				firstPtr++
				endPtr--
			}
		}
	}

	firstPtr = 0
	endPtr := len(vals) - 1
	for firstPtr < endPtr {
		tempVal := vals[firstPtr]
		vals[firstPtr] = vals[endPtr]
		vals[endPtr] = tempVal
		firstPtr++
		endPtr--
	}
	return vals
}

func reverseWordsWithStack(vals []string) []string {
	wordStack := [][]string{}
	currentWord := []string{}

	for _, ch := range vals {
		// If we haven't hit the end of the word go ahead and build up the current word
		if ch != " " {
			currentWord = append(currentWord, ch)
		} else {
			// We've hit whitespace and thus the end of the current word
			// Add the current word to the stack
			wordStack = append(wordStack, currentWord)
			// Add the white space character to the stack
			wordStack = append(wordStack, []string{ch})
			// Reset the current word
			currentWord = []string{}
		}
	}
	if len(currentWord) > 0 {
		wordStack = append(wordStack, currentWord)
	}

	var result []string
	for i := len(wordStack) - 1; i >= 0; i-- {
		result = append(result, wordStack[i]...)
	}
	return result
}

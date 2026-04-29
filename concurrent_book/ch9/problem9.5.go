package main

func main() {
	quit := make(chan int)
	Drain[int](quit,
		Print[int](quit,
			TakeUntil[int](LessThanEqualTo(1_000), quit,
				GenerateSquares(quit))))
	<-quit
}

func LessThanEqualTo(s int) func(int) bool {
	return func(n int) bool { return n <= s }
}

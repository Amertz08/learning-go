package main

func main() {
	quit := make(chan int)
	Drain[int](quit,
		Print[int](quit,
			TakeUntil[int](func(s int) bool { return s <= 1_000 }, quit,
				GenerateSquares(quit))))
	<-quit
}

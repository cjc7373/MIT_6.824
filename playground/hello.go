package main

import "fmt"

func main() {
	count := 0

	for i := 0; i < 10; i++ {
		go func() {
			count++
		}()
	}

	if count < 5 {
	}

	fmt.Println(count)
}

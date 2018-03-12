package main

import (
	"fmt"
)

func main() {
	ch := make(chan string)
	for i := 0; i < 5000; i++ {
		go printHello(i, ch)
	}

	for {
		msg := <-ch
		fmt.Println(msg)
	}
}

func printHello(i int, ch chan string) {
	for {
		ch <- fmt.Sprintf("Hello Zack!"+
			"form goroutine %d \n", i)
	}
}

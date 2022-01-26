package main

import "fmt"

func main() {
	c := f3(f1("Hello"), f2("World"))
	fmt.Println(<-c)
}

func f3(c1 chan string, c2 chan string) chan string {
	c := make(chan string, 100)
	select {
	case msg1 := <-c1:
		c <- msg1
	case msg2 := <-c2:
		c <- msg2
	}
	return c
}

func f1(in string) chan string {
	c := make(chan string, 10)
	c <- in
	return c
}

func f2(out string) chan string {
	c := make(chan string, 10)
	c <- out
	return c
}

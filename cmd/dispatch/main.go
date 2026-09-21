package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println("dispatch 0.1.0-dev")
		return
	}
	fmt.Fprintln(os.Stderr, "usage: dispatch --version")
	os.Exit(2)
}

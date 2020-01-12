package main

import (
	"fmt"
)

func main() {
	test := make(map[string]int)
	test["course"]++
	print(test)
}

func print(test map[string]int) {
	fmt.Println(test["course"])

}

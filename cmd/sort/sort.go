package main

import (
	"fmt"
	"sort"
)

func main() {
	arr := []int{3, 6, 9, 7, 0, 4}
	sort.Ints(arr)

	for _, v := range arr {
		fmt.Println(v)
	}

}

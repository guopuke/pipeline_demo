package main

import (
	"fmt"
	"googuu/pipeline"
	"os"
	"bufio"
	"time"
)

func main() {
	//for {
	//	if num, ok := <-p; ok {
	//		fmt.Println(num)
	//	} else {
	//		break
	//	}
	//}

	//mergeDemo()
	const fileName = "small.in"
	const num = 64

	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.RandomSource(num)

	writer := bufio.NewWriter(file)
	pipeline.WriterSink(bufio.NewWriter(file), p)
	writer.Flush()

	file, err = os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p = pipeline.ReaderSource(bufio.NewReader(file), -1)
	count := 0

	for v := range p {
		fmt.Println(v)
		count++
		if count > 100 {
			break
		}
	}
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
}

func mergeDemo() {
	p1 := pipeline.ArraySource(3, 1, 5, 1, 6, 7, 3, 9)
	p1Sort := pipeline.InMemSort(p1)
	p2 := pipeline.ArraySource(3, 1, 5, 1, 6, 7, 3, 9)
	p2Sort := pipeline.InMemSort(p2)
	pMergeResult := pipeline.Merge(p1Sort, p2Sort)
	for v := range pMergeResult {
		fmt.Println(v)
	}
}

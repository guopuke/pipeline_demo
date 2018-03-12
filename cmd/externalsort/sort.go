package main

import (
	"os"
	"googuu/pipeline"
	"bufio"
	"fmt"
	"strconv"
	"time"
)

func main() {
	infilename := "small.in"
	outfilename := "small.out"
	p := createNetworkPipeline(infilename, 512, 4)
	time.Sleep(time.Hour)
	writeToFile(p, outfilename)
	printFile(outfilename)

	//filePipeline()
}

func filePipeline() {
	infilename := "small.in"
	outfilename := "small.out"
	p := createPipeline(infilename, 512, 4)
	writeToFile(p, outfilename)
	printFile(outfilename)
	fmt.Println("down")
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReaderSource(file, -1)
	for v := range p {
		fmt.Println(v)
	}

}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	pipeline.WriterSink(writer, p)
}

func createPipeline(
	filename string, fileSize, chunkCount int) <-chan int {
	chunkSize := fileSize / chunkCount
	var sortResults []<-chan int
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		sortResults = append(sortResults,
			pipeline.InMemSort(source))
	}

	return pipeline.MergeN(sortResults...)
}

func createNetworkPipeline(
	filename string, fileSize, chunkCount int) <-chan int {
	chunkSize := fileSize / chunkCount
	var sortAddr []string
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		addr := ":" + strconv.Itoa(7000+i)
		pipeline.NetworkSink(addr, pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}
	var sortResults []<-chan int
	for _, addr := range sortAddr {
		sortResults = append(sortResults, pipeline.NetworkSource(addr))
	}
	return pipeline.MergeN(sortResults...)
}

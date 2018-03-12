package pipeline

import "sort"
import "fmt"
import "io"
import "encoding/binary"
import "math/rand"
import "time"

var startTime time.Time

func Init() {
	startTime = time.Now()
}

// 将数组里的数据传入到通道里
func ArraySource(a ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, v := range a {
			out <- v
			fmt.Println("I'm ranging num -->", v)
		}
		close(out)
	}()

	return out
}

// 通道内进行排序
func InMemSort(in <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		// Read into memory
		a := []int{}
		for v := range in {
			fmt.Println("I'm wait sorting...", v)
			a = append(a, v)
		}
		fmt.Printf("Read down", time.Now().Sub(startTime))

		// Sort
		sort.Ints(a)

		// Output
		for _, v := range a {
			out <- v
		}

		close(out)
	}()

	return out
}

// 归并
func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		// ok1 或者 ok2 有数据时进行循环
		for ok1 || ok2 {
			// 如果 v2 没有数据或者 v1 有数据并且小于等于 v2,将 v1 输出到 channel
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		fmt.Printf("Merge down", time.Now().Sub(startTime))

		close(out)
	}()

	return out
}

// 读取源文件
// 分块读取 chunkSize 为-1时,全部读取
func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		buffer := make([]byte, 8)
		bytesRead := 0
		for {
			n, err := reader.Read(buffer)
			bytesRead += n
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			if err != nil || (chunkSize != -1 && bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()

	return out
}

// 写数据
func WriterSink(writer io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(
			buffer, uint64(v))
		writer.Write(buffer)
	}
}

func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()

	return out
}

// 搭建归并节点组
func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	m := len(inputs) / 2

	// merge inputs[0..m) and inputs[m..end)
	return Merge(
		MergeN(inputs[:m]...),
		MergeN(inputs[m:]...),
	)
}

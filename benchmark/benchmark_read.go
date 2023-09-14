package main

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"time"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

func setup() string {
	log_folder, _ := os.MkdirTemp("", "log_folder")
	return path.Join(log_folder, "test.bin")
}

func destory(path string) {
	os.RemoveAll(path)
}

func write(data_file string) {
	fd, err := os.OpenFile(data_file, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic(err)
	}
	// Step 1: Write 1GB file
	for i := 0; i < KB; i++ {
		_, err := fd.Write(make([]byte, MB))
		if err != nil {
			panic(err)
		}
	}
	fd.Sync()
	fd.Close()
	fmt.Println("Done writing ", data_file)
}

func seq_read(data_file string) {
	fd, err := os.OpenFile(data_file, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	now := time.Now()
	for i := 0; i < MB; i++ {
		buffer := make([]byte, KB)
		fd.Read(buffer)
	}
	fmt.Println("Sequential read ", time.Since(now))
	fd.Close()
}

func rand_read(data_file string) {
	fd, err := os.OpenFile(data_file, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	r := rand.New(rand.NewSource(20))
	var offsets []int
	for i := 0; i < MB; i++ {
		offsets = append(offsets, r.Intn(MB-2*KB))
	}

	fmt.Println(len(offsets))
	now := time.Now()
	for _, offset := range offsets {
		buffer := make([]byte, KB)
		_, err = fd.ReadAt(buffer, int64(offset))
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Random read ", time.Since(now))
	fd.Close()
}

func main() {
	data_file := setup()
	write(data_file)
	// Step 2: Sequential read
	seq_read(data_file)
	// data_file := "/tmp/log_folder3812176505/test.bin"
	// Step 3: Random read
	rand_read(data_file)
	fmt.Println("Done")
}

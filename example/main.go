package main

import (
	"fmt"
	"io"

	"github.com/yncxcw/wal"
)

func main() {

	logger, err := wal.Open(wal.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// Sequential write
	logger.Write([]byte("hello"))
	logger.Write([]byte("world"))
	logger.Write([]byte("writing"))
	logger.Write([]byte("logs"))

	fmt.Printf("There are %d logs \n", logger.GetLogCount())
	// Random read
	log, _ := logger.Read(uint64(1))
	fmt.Printf("Reading log %s  at index %d \n", string(log), 1)

	// Sequential read
	loggerIter := wal.NewWLogIterator(logger)
	for {
		log, index, err := loggerIter.Next()
		if err == io.EOF {
			break
		}
		fmt.Printf("Iterating log %s at index %d \n", string(log), index)
	}
}

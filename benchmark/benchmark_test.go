package benchmark

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/yncxcw/wal"
)

func Setup() *wal.WLog {
	log_folder, _ := os.MkdirTemp("", "log_folder")
	opts := wal.Options{
		DirPath:     log_folder,
		FsSync:      true,
		SegmentSize: 5 * wal.MB,
		CacheSize:   1 * wal.MB,
		BytesToSync: wal.KB,
		//readWithCRC: true,
	}
	log, _ := wal.Open(opts)
	return log
}

func Destroy(log *wal.WLog) {
	if log != nil {
		log.Close()
		os.RemoveAll(log.Path())
	}
}

func BenchmarkWrite100K(t *testing.B) {
	logger := Setup()
	defer Destroy(logger)
	buffer := make([]byte, 100*wal.KB)

	t.ResetTimer()
	t.ReportAllocs()
	for i := 0; i < 1000; i++ {
		logger.Write(buffer)
	}
}

func BenchmarkWrite10bytes(t *testing.B) {
	logger := Setup()
	defer Destroy(logger)
	buffer := make([]byte, 10)

	t.ResetTimer()
	t.ReportAllocs()
	for i := 0; i < 1000; i++ {
		logger.Write(buffer)
	}
}

func BenchmarkRandRead100K(t *testing.B) {
	logger := Setup()
	defer Destroy(logger)
	buffer := make([]byte, 100*wal.KB)

	numLogs := 1000
	for i := 0; i < numLogs; i++ {
		logger.Write(buffer)
	}

	var indices []uint64
	r := rand.New(rand.NewSource(33))

	for i := 0; i < 1000; i++ {
		indices = append(indices, uint64(r.Int63n(int64(numLogs))))
	}

	var bench_fn = func(b *testing.B) {
		for i := 0; i < 1000; i++ {
			now := time.Now()
			_, err := logger.Read(indices[i])
			fmt.Println("One rand read ", time.Since(now))
			if err != nil {
				b.Fatal(err)
			}
		}

	}
	t.ResetTimer()
	t.ReportAllocs()
	t.Run("RandRead100K", bench_fn)
}

func BenchmarkSequentalRead100K(t *testing.B) {
	logger := Setup()
	defer Destroy(logger)
	buffer := make([]byte, 100*wal.KB)

	numLogs := 1000
	for i := 0; i < numLogs; i++ {
		logger.Write(buffer)
	}

	logIter := wal.NewWLogIterator(logger)

	var bench_fn = func(b *testing.B) {
		for {
			now := time.Now()
			_, _, err := logIter.Next()
			fmt.Println("One sequential read ", time.Since(now))
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	t.ResetTimer()
	t.ReportAllocs()
	t.Run("SequentialRead100K", bench_fn)

}

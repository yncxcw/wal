package wal

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func DestryWLog(log *WLog) {
	if log != nil {
		log.Close()
		os.RemoveAll(log.path)
	}
}

func CraeteWLog(segmentSize uint64, cacheSize uint64) *WLog {
	log_folder, _ := os.MkdirTemp("", "log_folder")
	opts := Options{
		DirPath:     log_folder,
		FsSync:      true,
		SegmentSize: segmentSize,
		CacheSize:   cacheSize,
		BytesToSync: KB,
		readWithCRC: true,
	}
	log, _ := Open(opts)
	return log
}

func TestOpenWLogAndWrite(t *testing.T) {
	log := CraeteWLog(MB, 0)
	defer DestryWLog(log)

	logPostion, err := log.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), logPostion.id)
	assert.Equal(t, uint64(0), logPostion.segmentIndex)
	// The segment header is 8 bytes
	assert.Equal(t, uint64(8), logPostion.logOffset)
	assert.Equal(t, uint32(5), logPostion.logSize)

	logPostion, err = log.Write([]byte("world!"))
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), logPostion.id)
	assert.Equal(t, uint64(0), logPostion.segmentIndex)
	// The segment header + log header + 5
	assert.Equal(t, uint64(21), logPostion.logOffset)
	assert.Equal(t, uint32(6), logPostion.logSize)
}

func TestWLogWriteAndRead(t *testing.T) {
	log := CraeteWLog(MB, 0)
	defer DestryWLog(log)

	var str = "abcdefghijklmnopq"
	for i := 1; i < len(str); i++ {
		_, err := log.Write([]byte(str[:i]))
		assert.Nil(t, err)
	}

	for i := 1; i < len(str); i++ {
		buffer, err := log.Read(uint64(i - 1))
		assert.Nil(t, err)
		assert.Equal(t, str[:i], string(buffer))
	}
}

func TestWLogWriteAndReOpen(t *testing.T) {
	log := CraeteWLog(10*KB, 0)

	// Firt wrte data to WLog
	for i := 0; i < 50; i++ {
		_, err := log.Write(make([]byte, 3*KB))
		assert.Nil(t, err)
	}
	path := log.path

	// Close it
	log.Close()

	opts := Options{
		DirPath:     path,
		FsSync:      true,
		SegmentSize: 10 * KB,
		CacheSize:   0,
		BytesToSync: KB,
		readWithCRC: true,
	}

	// Reopen it for reading and writing
	log, err := Open(opts)
	assert.Nil(t, err)

	assert.Equal(t, uint64(50), log.GetLogCount())
	for i := 0; i < 50; i++ {
		_, err := log.Read(uint64(i))
		assert.Nil(t, err)
	}

	// Write 50 more new logs
	for i := 0; i < 50; i++ {
		_, err := log.Write(make([]byte, 3*KB))
		assert.Nil(t, err)
	}
	// Read totall 100 logs
	for i := 0; i < 100; i++ {
		_, err := log.Read(uint64(i))
		assert.Nil(t, err)
	}

	DestryWLog(log)
}

func TestMultipleSegments(t *testing.T) {
	log := CraeteWLog(10*KB+8, 0)
	defer DestryWLog(log)

	for i := 0; i < 20; i++ {
		buffer := make([]byte, 2*KB-8)
		logPos, err := log.Write(buffer)
		assert.Nil(t, err)
		assert.Equal(t, uint64((i*2)/10), logPos.segmentIndex)
	}

	for i := 0; i < 20; i++ {
		_, err := log.Read(uint64(i))
		assert.Nil(t, err)
		assert.LessOrEqual(t, uint64(i), log.GetLogCount())
	}
}

func TestMultipleReaderAndWriter(t *testing.T) {
	log := CraeteWLog(10*MB, 0)
	defer DestryWLog(log)

	// Writing goroutins
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		i := i
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := log.Write([]byte(fmt.Sprintf("log message %d from process %d", j, i)))
				assert.Nil(t, err)
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, uint64(200), log.GetLogCount())

	// Reading goroutins
	for i := 0; i < 20; i++ {
		wg.Add(1)
		i := i
		go func(i int) {
			defer wg.Done()
			for j := i * 10; j < (i+1)*10; j++ {
				buffer, err := log.Read(uint64(j))
				assert.Nil(t, err)
				match, _ := regexp.MatchString(`log message \d+ from process \d+`, string(buffer))
				assert.True(t, match)
			}
		}(i)
	}
	wg.Wait()
}

func TestWLogIteratorE2E(t *testing.T) {
	log := CraeteWLog(10*KB, 0)
	defer DestryWLog(log)

	// Dump some data
	for i := 0; i < 1000; i++ {
		_, err := log.Write(make([]byte, 3*KB))
		assert.Nil(t, err)
	}

	logIterator := NewWLogIterator(log)
	var expectedLogIndex uint64 = 0
	for {
		data, logIndex, err := logIterator.Next()
		if err == io.EOF {
			break
		}

		assert.Nil(t, err)
		assert.Equal(t, 3*KB, len(data))
		assert.Equal(t, expectedLogIndex, logIndex)
		expectedLogIndex++
	}
	assert.Equal(t, log.GetLogCount(), expectedLogIndex)

}

func TestWriteAndRandReadE2E(t *testing.T) {
	// With cache enabled
	log := CraeteWLog(10*KB, 0)
	defer DestryWLog(log)

	r := rand.New(rand.NewSource(33))
	numLogs := 1000
	// Write logs with variable sizes.
	logMap := make(map[uint64]string)
	logWord := []string{"a", "b", "c", "d", "e", "f", "g"}
	for i := 0; i < numLogs; i++ {
		logCharactor := logWord[int(r.Int31n(int32(len(logWord))))]
		logLength := 1 + r.Int31n(int32(10*KB-1))
		logMessage := strings.Repeat(logCharactor, int(logLength))
		_, err := log.Write([]byte(logMessage))
		logMap[uint64(i)] = logMessage
		assert.Nil(t, err)
	}

	log.sync()
	// Read logs with variable log message
	for i := 0; i < 1000; i++ {
		logIndex := uint64(r.Int63n(int64(numLogs)))
		buffer, err := log.Read(logIndex)
		assert.Nil(t, err)
		assert.Equal(t, logMap[logIndex], string(buffer))
	}
}

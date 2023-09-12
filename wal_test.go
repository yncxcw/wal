package wal

import (
	"fmt"
	"os"
	"regexp"
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

func CraeteWLog(segmentSize uint64) *WLog {
	log_folder, _ := os.MkdirTemp("", "log_folder")
	opts := Options{
		DirPath:          log_folder,
		FsSync:           true,
		SegmentSize:      segmentSize,
		SegmentCacheSize: KB,
		BytesToSync:      KB,
		readWithCRC:      true,
	}
	log, _ := Open(opts)
	return log
}

func TestOpenWLogAndWrite(t *testing.T) {
	log := CraeteWLog(MB)
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
	log := CraeteWLog(MB)
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

func TestMultipleSegments(t *testing.T) {
	log := CraeteWLog(10*KB + 8)
	defer DestryWLog(log)

	for i := 0; i < 10; i++ {
		buffer := make([]byte, 2*KB-8)
		logPos, err := log.Write(buffer)
		assert.Nil(t, err)
		assert.Equal(t, uint64((i*2)/10), logPos.segmentIndex)
	}

	for i := 0; i < 10; i++ {
		_, err := log.Read(uint64(i))
		assert.Nil(t, err)
		assert.LessOrEqual(t, uint64(i), log.GetCurrentLogIndex())
	}
}

func TestMultipleReaderAndWriter(t *testing.T) {
	log := CraeteWLog(10 * MB)
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
	assert.Equal(t, uint64(200), log.GetCurrentLogIndex())

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

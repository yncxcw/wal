package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func DestryWLog(log *WLog) {
	if log != nil {
		log.Close()
		os.RemoveAll(log.path)
	}
}

func TestOpenWLogAndWrite(t *testing.T) {
	log_folder, _ := os.MkdirTemp("", "log_folder")
	opts := Options{
		DirPath:          log_folder,
		FsSync:           true,
		SegmentSize:      100 * MB,
		SegmentCacheSize: 1 * MB,
		BytesToSync:      500 * KB,
	}
	log, err := Open(opts)
	defer DestryWLog(log)
	assert.Nil(t, err)

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

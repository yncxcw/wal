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

	_, err = log.Write([]byte("hello"))
	assert.Nil(t, err)

	_, err = log.Write([]byte("world"))
	assert.Nil(t, err)

}

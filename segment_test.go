package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func DestrySegment(segment *Segment) {
	if segment != nil {
		segment.Close()
		os.RemoveAll(segment.path)
	}
}

func CraeteSegment() *Segment {
	log_folder, _ := os.MkdirTemp("", "log_folder")
	segment, _ := NewSegment(log_folder, uint64(0), uint64(0), true)
	return segment
}

func TestSegmentPath(t *testing.T) {
	path := SegmentPath(uint64(0), "/foo/boo")
	assert.Equal(t, "/foo/boo/segment_0.bin", path)

	path = SegmentPath(uint64(1), "")
	assert.Equal(t, "segment_1.bin", path)

	path = SegmentPath(uint64(999), "/foo")
	assert.Equal(t, "/foo/segment_999.bin", path)
}

func TestWriteSingleLog(t *testing.T) {
	data := []byte("hello")
	buffer := WriteSingleLog(data)
	assert.Equal(t, len(buffer), len(data)+8)

	assert.Equal(t, uint32(len(data)), binary.LittleEndian.Uint32(buffer[0:4]))
	assert.Equal(t, crc32.ChecksumIEEE(data), binary.LittleEndian.Uint32(buffer[4:8]))
}

func TestWriteSegmentAndRead(t *testing.T) {
	segment := CraeteSegment()
	defer DestrySegment(segment)

	// Make sure sugment file is there
	_, err := os.Stat(segment.path)
	assert.Nil(t, err)
	var str string
	str = "abcdefg"
	var logOffset uint64
	logOffset = 8
	for i := 1; i < len(str); i++ {
		logPostion, err := segment.Write([]byte(str[:i]))
		assert.Nil(t, err)
		err = segment.Sync()
		assert.Nil(t, err)
		assert.Equal(t, uint64(i-1), logPostion.id)
		assert.Equal(t, uint64(0), logPostion.segmentIndex)
		assert.Equal(t, logOffset, logPostion.logOffset)
		assert.Equal(t, uint32(i), logPostion.logSize)
		logOffset += uint64(8 + i)
	}

	for i := 1; i < len(str); i++ {
		buffer, err := segment.Read(uint64(i-1), true)
		assert.Nil(t, err)
		assert.Equal(t, str[:i], string(buffer))
	}
}

func TestSegmentSize(t *testing.T) {
	segment := CraeteSegment()
	defer DestrySegment(segment)

	for i := 1; i <= 10; i++ {
		buffer := make([]byte, 10*KB)
		_, err := segment.Write(buffer)
		assert.Nil(t, err)

		err = segment.Sync()
		assert.Nil(t, err)
		size := segment.Size()
		// (10KB log + 8 byte log) * # of log + 8 byte segment header
		assert.Equal(t, uint64((10*KB+8)*i+8), size)
	}
}

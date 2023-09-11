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

func TestWriteSegmentAndWrite(t *testing.T) {
	segment := CraeteSegment()
	defer DestrySegment(segment)
	logPostion, err := segment.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), logPostion.id)
	assert.Equal(t, uint64(0), logPostion.segmentIndex)
	// The segment header is 8 bytes
	assert.Equal(t, uint64(8), logPostion.logOffset)
	assert.Equal(t, uint32(5), logPostion.logSize)

	logPostion, err = segment.Write([]byte("world!"))
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), logPostion.id)
	assert.Equal(t, uint64(0), logPostion.segmentIndex)
	// The segment header + log header + 5
	assert.Equal(t, uint64(21), logPostion.logOffset)
	assert.Equal(t, uint32(6), logPostion.logSize)

}

package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

const (
	// 8 bytes for the header size.
	// Checksum | Length  | data
	//    4          4       ...
	logHeaderSize = 8

	// 8 bytes for the header size of a segment file
	// start log index | logi, logi+1 ...
	//       8
	SegHeaderSize = 8

	fileModePerm = 0644
)

type Options struct {
	//  Enforce fsync after every write.
	FsSync bool
	// Segment size of each Segment.
	SegmentSize int
	// The maximum number of segments that will be kept in memory.
	SegmentCacheSize int
}

var DefaultOptions = &Options{
	FsSync:           true,      // Sync for every write
	SegmentSize:      202971520, // 20 MB log segment file.
	SegmentCacheSize: 10,        // number of segments in memory.
}

type WLog struct {
	mu             sync.RWMutex //Readwrite lock for accessing logs
	path           string       // Absolute path to the log directory
	options        Options      // Options of the WLog.
	currentSegment *Segment     // pints to the current segment
	closed         bool         // If the log is closed.
}

type Segment struct {
	path             string   // Paht of the segment file.
	id               uint64   // The segment id
	fd               *os.File // The actual file of the segment.
	header           []byte   // Temporary buffer for header.
	logStartIndex    uint64   // First index log in the segment.
	logCurrentIndex  uint64   // The current log index.
	logCurrentOffset uint64   // The current offset of log.
	closed           bool     // If the segment is closed or not
	size             uint32   // The current size of the segment.
	fsync            bool     // If true, do fsync for each write.
}
type LogPosition struct {
	id           uint64 // The global index of the the log message.
	segmentIndex uint64 // The index of the segment to write the log message.
	logOffset    uint64 // The start offset of the log message in the segment file.
	logSize      uint32 // How many bytes the log message takes in the segment file.
}

func (seg *Segment) Close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true
	return seg.fd.Close()
}

func (seg *Segment) WriteBuffer(buffer []byte) error {
	if seg.closed {
		return nil
	}

	if _, err := seg.fd.Write(buffer); err != nil {
		return err
	}

	if seg.fsync {
		seg.fd.Sync()
	}
	return nil
}
func NewSegment(segmentFolder string, segmentId uint64, logStartIndex uint64, fsync bool) (*Segment, error) {
	segmentPath := SegmentPath(segmentId, segmentFolder)
	fd, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, fileModePerm)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, os.SEEK_END)

	if err != nil {
		return nil, err
	}

	buf := make([]byte, SegHeaderSize)
	binary.LittleEndian.PutUint64(buf, logStartIndex)
	if _, err = fd.Write(buf); err != nil {
		return nil, err
	}
	if err = fd.Sync(); err != nil {
		return nil, err
	}

	return &Segment{
		path:            segmentPath,
		id:              segmentId,
		fd:              fd,
		header:          make([]byte, logHeaderSize),
		logStartIndex:   logStartIndex,
		logCurrentIndex: logStartIndex,
		size:            0,
		fsync:           fsync,
	}, nil
}

func (seg *Segment) Write(data []byte) (*LogPosition, error) {
	// Write the log
	var buffer []byte
	WriteSingleLog(buffer, data)
	err := seg.WriteBuffer(buffer)
	if err != nil {
		return nil, err
	}
	// Update stats
	seg.logCurrentIndex += 1
	seg.logCurrentOffset += uint64(len(buffer))

	return &LogPosition{
		id:           seg.logCurrentIndex,
		segmentIndex: seg.id,
		logOffset:    seg.logCurrentOffset,
		logSize:      uint32(len(data)),
	}, nil
}

// Append data to buffer with header
func WriteSingleLog(buffer []byte, data []byte) {
	sum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(buffer, sum)
	binary.LittleEndian.PutUint32(buffer, uint32(len(data)))
	buffer = append(buffer, data...)
}

//Return the segment Path.
func SegmentPath(segmentId uint64, segmentFolder string) string {
	return filepath.Join(segmentFolder, fmt.Sprintf("segment_%d.bin", segmentId))
}

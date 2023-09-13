package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	// 8 bytes for the header size.
	// Length | Checksum  | data
	//    4          4       ...
	LogHeaderSize = 8

	// 8 bytes for the header of a segment file
	// start log index | logi, logi+1 ...
	//       8
	SegHeaderSize = 8

	fileModePerm = 0644

	startSegmentId = 0

	// Const size.
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var (
	ErrClosed = errors.New("The segment is closed.")
	ErrCRC    = errors.New("The CRC missmatch error.")
	ErrNilLog = errors.New("Loading Empty log.")
)

type Options struct {
	// The dir of the log folder
	DirPath string
	//  Enforce fsync after every write.
	FsSync bool
	// Segment size of each Segment.
	SegmentSize uint64
	// The maximum number of logs in memory.
	CacheSize uint64
	// For writing how many bytes before it syncs.
	BytesToSync uint64
	// Read with CRC.
	readWithCRC bool
}

var DefaultOptions = Options{
	DirPath:     filepath.Join(os.TempDir(), "wal"), // Data drectory
	FsSync:      true,                               // Sync for every write
	SegmentSize: 20 * MB,                            // 20 MB log segment file.
	CacheSize:   50,                                 // Size of cached logs in terms of numbers of logs.
	BytesToSync: KB,                                 // How many bytes accumlated to call fsync.
	readWithCRC: true,
}

type WLog struct {
	mu             sync.RWMutex               // Readwrite lock for accessing logs
	path           string                     // Absolute path to the log directory
	options        Options                    // Options of the WLog.
	currentSegment *Segment                   // pints to the current segment
	readSegment    map[uint64]*Segment        // The segments older and currentSegment, read-only.
	cache          *lru.Cache[uint64, []byte] // The cache for recent logs.
	closed         bool                       // If the log is closed.
	byteWrittent   int                        // Bytes written before fsync.
}

// Used to iterate the WLog
type WLogIterator struct {
	segmentIterators []*SegmentIterator // The segments to iterate through.
	currentSegment   int                // The index of current segment from segmentIterators.
}

// Used to iterate the Segment
type Segment struct {
	path             string   // Paht of the segment file.
	id               uint64   // The segment id
	fd               *os.File // The actual file of the segment.
	header           []byte   // Temporary buffer for header.
	logStartIndex    uint64   // First index log in the segment.
	logCurrentIndex  uint64   // The current log index, it always points to the index of the next log to write
	logCurrentOffset uint64   // The current offset of log.
	closed           bool     // If the segment is closed or not
	fsync            bool     // If true, do fsync for each write.
}

// Iterator to read a segment.
type SegmentIterator struct {
	segment          *Segment // The segment to read.
	logCurrentIndex  uint64   // The current log to read.
	logCurrentOffset uint64   // The current offset of the log to read.
}
type LogPosition struct {
	id           uint64 // The global index of the the log message.
	segmentIndex uint64 // The index of the segment to write the log message.
	logOffset    uint64 // The start offset of the log message in the segment file.
	logSize      uint32 // How many bytes the log message takes in the segment file.
}

func Open(options Options) (*WLog, error) {
	if options.SegmentSize > uint64(10*GB) {
		return nil, fmt.Errorf("Segment size %d exceeds 10GB.", options.SegmentSize)
	}

	wal := &WLog{
		options:     options,
		path:        options.DirPath,
		readSegment: make(map[uint64]*Segment),
	}

	if options.CacheSize > 0 {
		wal.cache, _ = lru.New[uint64, []byte](int(options.CacheSize))
	}

	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	// Read the log folder and open all segment files.
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	var segmentIds []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf("Get valid segment file %s", entry.Name())
		}

		var segmentId uint64
		_, err := fmt.Sscanf(entry.Name(), "segment_%d.bin", &segmentId)
		if err != nil {
			return nil, err
		}

		segmentIds = append(segmentIds, segmentId)
	}
	if len(segmentIds) == 0 {
		segment, err := NewSegment(options.DirPath, startSegmentId, 0, options.FsSync)

		if err != nil {
			return nil, err
		}
		wal.currentSegment = segment
	} else {
		sort.Slice(segmentIds, func(i, j int) bool { return segmentIds[i] < segmentIds[j] })
		for i, segmentId := range segmentIds {
			segment, err := OpenSegment(options.DirPath, segmentId, options.FsSync)

			if err != nil {
				return nil, err
			}
			if i == len(segmentIds)-1 {
				wal.currentSegment = segment
				// For the active segenment, we need to iterate it find the current log index
				sIter := NewSegmentIterator(wal.currentSegment)
				for {
					_, index, err := sIter.Next()
					if err == io.EOF {
						wal.currentSegment.logCurrentIndex = index
						break
					}
					if err != nil {
						return nil, err
					}
				}
			} else {
				wal.readSegment[segmentId] = segment
			}
		}
	}

	return wal, nil
}

// New for writing
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
		path:             segmentPath,
		id:               segmentId,
		fd:               fd,
		header:           make([]byte, SegHeaderSize),
		logStartIndex:    logStartIndex,
		logCurrentIndex:  logStartIndex,
		logCurrentOffset: SegHeaderSize,
		fsync:            fsync,
	}, nil
}

// Open existing segments for reading
func OpenSegment(segmentFolder string, segmentId uint64, fsync bool) (*Segment, error) {
	segmentPath := SegmentPath(segmentId, segmentFolder)
	fd, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_APPEND, fileModePerm)
	if err != nil {
		return nil, err
	}

	_, err = fd.Seek(0, os.SEEK_END)
	//Read in segment header
	buf := make([]byte, SegHeaderSize)
	_, err = fd.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}
	logStartIndex := binary.LittleEndian.Uint64(buf)
	//Get segment size
	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	return &Segment{
		path:             segmentPath,
		id:               segmentId,
		fd:               fd,
		header:           buf,
		logStartIndex:    logStartIndex,
		logCurrentIndex:  0,
		logCurrentOffset: uint64(stat.Size()),
		fsync:            fsync,
	}, nil
}

func (wal *WLog) getCurrentSegmentId() uint64 {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return wal.currentSegment.id
}

func (wal *WLog) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	wal.sync()
	if wal.cache != nil {
		wal.cache.Purge()
	}
	for _, segment := range wal.readSegment {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	if err := wal.currentSegment.Close(); err != nil {
		return err
	}
	return nil
}

func (wal *WLog) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return len(wal.readSegment) == 0 && wal.currentSegment.Size() == 0
}

func (wal *WLog) GetLogCount() uint64 {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return wal.currentSegment.logCurrentIndex
}

//Called needs to make sure it is locked.
func (wal *WLog) isFull(size int) bool {
	if wal.currentSegment.Size()+LogHeaderSize+uint64(size) > wal.options.SegmentSize {
		return true
	}
	return false
}

// The caller needs to make sure it is under locked.
func (wal *WLog) sync() error {
	if err := wal.currentSegment.Sync(); err != nil {
		return err
	}
	wal.byteWrittent = 0
	return nil
}

func (wal *WLog) Path() string {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return wal.path
}

func (wal *WLog) Write(data []byte) (*LogPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// If the data to write is larger than the SegmentSize
	if len(data)+LogHeaderSize > int(wal.options.SegmentSize) {
		return nil, fmt.Errorf("data %d to write is larger than SegmentSize", len(data))
	}

	// If the current segment is full.
	if wal.isFull(len(data)) {
		if err := wal.sync(); err != nil {
			return nil, err
		}

		// Note the new segment starts with wal.currentSegment.logCurrentIndex
		newSegment, err := NewSegment(wal.path, wal.currentSegment.id+1, wal.currentSegment.logCurrentIndex, wal.options.FsSync)
		if err != nil {
			return nil, err
		}
		wal.readSegment[wal.currentSegment.id] = wal.currentSegment
		wal.currentSegment = newSegment
	}

	position, err := wal.currentSegment.Write(data)
	if err != nil {
		return nil, err
	}
	wal.byteWrittent += int(position.logSize)

	needSync := wal.options.FsSync
	if !wal.options.FsSync && wal.options.BytesToSync > 0 {
		needSync = needSync && wal.byteWrittent >= int(wal.options.BytesToSync)
	}
	if needSync {
		if err := wal.sync(); err != nil {
			return nil, err
		}
	}

	if wal.cache != nil {
		cacheBuffer := make([]byte, len(data))
		copy(cacheBuffer, data)
		wal.cache.Add(position.id, cacheBuffer)
	}

	return position, err
}

func (wal *WLog) Read(logIndex uint64) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	//First try to read from cache
	if wal.cache != nil {
		if buffer, ok := wal.cache.Get(logIndex); ok {
			return buffer, nil
		}
	}
	if logIndex >= wal.currentSegment.logStartIndex {
		return wal.currentSegment.Read(logIndex, wal.options.readWithCRC)
	} else {
		segmentIds := make([]uint64, len(wal.readSegment))
		for segmentId := range wal.readSegment {
			segmentIds = append(segmentIds, segmentId)
		}
		sort.Slice(segmentIds, func(i, j int) bool { return segmentIds[i] < segmentIds[j] })
		// Binary search to find the first segment whose logStartIndex >= longIndex
		var _serach_func = func(i int) bool {
			return logIndex < wal.readSegment[segmentIds[i]].logStartIndex
		}
		i := sort.Search(
			len(segmentIds),
			_serach_func,
		)
		return wal.readSegment[segmentIds[i-1]].Read(logIndex, wal.options.readWithCRC)

	}
}

func (seg *Segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

func (seg *Segment) Close() error {
	if seg.closed {
		return ErrClosed
	}
	seg.closed = true
	return seg.fd.Close()
}

func (seg *Segment) Size() uint64 {
	fi, err := seg.fd.Stat()
	if err != nil {
		panic(err)
	}
	return uint64(fi.Size())
}

func (seg *Segment) WriteBuffer(buffer []byte) error {
	if seg.closed {
		return ErrClosed
	}

	if _, err := seg.fd.Write(buffer); err != nil {
		return err
	}

	if seg.fsync {
		seg.fd.Sync()
	}
	return nil
}

func (seg *Segment) Write(data []byte) (*LogPosition, error) {
	// Write the log
	buffer := WriteSingleLog(data)
	err := seg.WriteBuffer(buffer)
	if err != nil {
		return nil, err
	}

	// Update stats
	defer func() {
		seg.logCurrentIndex += 1
		seg.logCurrentOffset += uint64(len(buffer))
	}()

	return &LogPosition{
		id:           seg.logCurrentIndex,
		segmentIndex: seg.id,
		logOffset:    seg.logCurrentOffset,
		logSize:      uint32(len(data)),
	}, nil
}

// Read the segment at logIndex.
func (seg *Segment) Read(logIndex uint64, checkCRC bool) ([]byte, error) {
	if seg.closed {
		return nil, ErrClosed
	}
	var logCurrentIndex = seg.logStartIndex
	var logCurrentOffset uint64 = SegHeaderSize
	for {
		log, err := seg.readSingLog(logCurrentOffset, checkCRC)
		if err != nil {
			return nil, err
		}
		if logCurrentIndex == logIndex {
			if len(log) == 0 {
				return nil, ErrNilLog
			}
			return log, nil
		}
		logCurrentIndex += 1
		logCurrentOffset += uint64(LogHeaderSize + len(log))
	}
}

func (seg *Segment) readSingLog(logOffset uint64, checkCRC bool) ([]byte, error) {
	// Read header.
	logHeader := make([]byte, LogHeaderSize)
	_, err := seg.fd.ReadAt(logHeader, int64(logOffset))
	if err != nil {
		return nil, err
	}
	logSize := binary.LittleEndian.Uint32(logHeader[:4])
	logCRC := binary.LittleEndian.Uint32(logHeader[4:])

	// Read log message.
	buffer := make([]byte, logSize)
	_, err = seg.fd.ReadAt(buffer, int64(logOffset+LogHeaderSize))
	if err != nil {
		return nil, err
	}
	// Check CRC if matching.
	if checkCRC {
		sum := crc32.ChecksumIEEE(buffer)
		if sum != logCRC {
			return nil, ErrCRC
		}
	}
	return buffer, nil
}

// Append data to buffer with header
func WriteSingleLog(data []byte) []byte {
	buffer := make([]byte, LogHeaderSize)
	sum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(buffer[0:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(buffer[4:], sum)
	buffer = append(buffer, data...)
	return buffer
}

//Return the segment Path.
func SegmentPath(segmentId uint64, segmentFolder string) string {
	return filepath.Join(segmentFolder, fmt.Sprintf("segment_%d.bin", segmentId))
}

func NewSegmentIterator(segment *Segment) *SegmentIterator {
	return &SegmentIterator{
		segment:          segment,
		logCurrentIndex:  segment.logStartIndex,
		logCurrentOffset: SegHeaderSize,
	}
}
func NewWLogIterator(wal *WLog) *WLogIterator {
	if wal == nil {
		return nil
	}
	var segmentIters []*SegmentIterator

	// First append the current segement.
	segmentIters = append(segmentIters, NewSegmentIterator(wal.currentSegment))

	// Then append the old segements.
	for _, segment := range wal.readSegment {
		segmentIter := NewSegmentIterator(segment)
		segmentIters = append(segmentIters, segmentIter)
	}

	sort.Slice(segmentIters, func(i, j int) bool { return segmentIters[i].segment.id < segmentIters[j].segment.id })

	return &WLogIterator{
		currentSegment:   0,
		segmentIterators: segmentIters,
	}
}

// Returns log message from the current pos.
func (wIter *WLogIterator) Next() ([]byte, uint64, error) {
	if wIter.currentSegment >= len(wIter.segmentIterators) {
		return nil, 0, io.EOF
	}
	data, logIndex, err := wIter.segmentIterators[wIter.currentSegment].Next()
	if err == io.EOF {
		wIter.currentSegment++
		return wIter.Next()
	}
	return data, logIndex, nil
}

func (sIter *SegmentIterator) Next() ([]byte, uint64, error) {
	log, err := sIter.segment.readSingLog(sIter.logCurrentOffset, true)

	if err != nil {
		return nil, sIter.logCurrentIndex, err
	}

	defer func() {
		sIter.logCurrentIndex += 1
		sIter.logCurrentOffset += uint64(len(log) + LogHeaderSize)
	}()
	return log, sIter.logCurrentIndex, nil
}

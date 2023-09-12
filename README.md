# Project introduction
Implementation of Write-Ahead logging using Golang. This project is insipred by [rosedblabs-wal](https://github.com/rosedblabs/wal). The major difference is that my implementation simplies the the process that splits a large log message into equal sized chunks.

# Log format
The log is organized as a list of segment files. Each segment file is capped by a configuration called `SegmentSize`.

Each segment file is encoded as follow:
```
// 8 bytes for the header of a segment file, it encodes the starting log index of this segment file.
Start log index (8 bytes) | log[i] | log[i+1] | log[i+2] ...
```

Each log message(the `log[i]` from the above illustration) is encoded as follow:
```
// 8 bytes for the header of a log message in which 4 bytes contribute to the Length of the log data and 4 bytes contribute to the checksum
Length(4 bytes) | Checksum(4 bytes)  | data (any length)
```

# Example usage
```go
logger, err := wal.Open(wal.DefaultOptions)

// Sequential write
logger.Write([]byte("hello"))
logger.Write([]byte("world"))
logger.Write([]byte("writing"))
logger.Write([]byte("logs"))

log, _ := logger.Read(uint64(1))
	
// Sequential read
loggerIter := wal.NewWLogIterator(logger)
for {
	log, index, err := loggerIter.Next()
	if err == io.EOF {
		break
	}

    // Write your code here to use the log data.
}
```

# Contact
For any question or usage, please contact ynjassionchen@gmail.com
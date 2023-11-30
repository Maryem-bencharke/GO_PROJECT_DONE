package main

import (
	"errors"
	"time"
	"sync"
	"fmt"
	"os"
	"bufio"
	"encoding/binary"
	"io"
)


type memDB struct {
	data []KeyValue
	wal  *WriteAheadLog
	mu   sync.Mutex 
	flushInterval time.Duration
	sstFileLoaded  bool
    setData   []KeyValue // Store Set operation data
	deleteData []KeyValue // Store Delete operation data
}
func (mem *memDB) SetFlushInterval(interval time.Duration) {
	mem.flushInterval = interval
}
func (mem *memDB) loadSSTFile(fileName string) error {
	if mem.sstFileLoaded {
        return nil
    }
    file, err := os.Open(fileName)
    if err != nil {
        return err
    }
    defer file.Close()

    reader := bufio.NewReader(file)
	 // Read checksum from the end of the SST file
	 checksum := calculateChecksum(mem.data)
	 _, err = file.Seek(-int64(binary.Size(checksum)), io.SeekEnd)
	 if err != nil {
		 return fmt.Errorf("error seeking checksum in SST file: %s", err)
	 }
 
	 var storedChecksum uint32
	 if err := binary.Read(file, binary.LittleEndian, &storedChecksum); err != nil {
		 return fmt.Errorf("error reading stored checksum from SST file: %s", err)
	 }
 
	 // Reset file offset to the beginning for reading key-value pairs
	 _, err = file.Seek(0, io.SeekStart)
	 if err != nil {
		 return fmt.Errorf("error resetting file offset in SST file: %s", err)
	 }
 
    for {
        // Read key length
        keyLenBytes := make([]byte, 4)
        _, err := reader.Read(keyLenBytes)
        if err != nil {
            break // Break loop at the end of the file or on error
        }
        keyLen := binary.LittleEndian.Uint32(keyLenBytes)

        // Read key data
        keyData := make([]byte, keyLen)
        _, err = reader.Read(keyData)
        if err != nil {
            break // Break loop at the end of the file or on error
        }

        // Read value length
        valueLenBytes := make([]byte, 4)
        _, err = reader.Read(valueLenBytes)
        if err != nil {
            break // Break loop at the end of the file or on error
        }
        valueLen := binary.LittleEndian.Uint32(valueLenBytes)

        // Read value data
        valueData := make([]byte, valueLen)
        _, err = reader.Read(valueData)
        if err != nil {
            break // Break loop at the end of the file or on error
        }

        // Append KeyValue pairs to mem.data
        mem.data = append(mem.data, KeyValue{
            Key:   keyData,
            Value: valueData,
        })
    }
	// Calculate checksum of loaded key-value pairs
    loadedChecksum := calculateChecksum(mem.data)

    // Compare checksums to validate file integrity
    if loadedChecksum != storedChecksum {
        return fmt.Errorf("SST file integrity check failed: checksums do not match")
    }
	mem.sstFileLoaded = true
    return nil
}
func NewMemDB(wal *WriteAheadLog) *memDB {
	mem := &memDB{
		data: make([]KeyValue, 0),
		wal:  wal,
	}
	go mem.periodicFlush()
	return mem
}

func (mem *memDB) Set(key, value []byte) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	entry := KeyValue{Key: key, Value: value}
	mem.wal.AppendEntry(Set, entry)
	mem.data = append(mem.data, entry)
	return nil
}

func (mem *memDB) Del(key []byte) ([]byte, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	for i, kv := range mem.data {
		if string(kv.Key) == string(key) {
			deletedValue := kv.Value
			mem.wal.AppendEntry(Delete, kv)
			mem.data = append(mem.data[:i], mem.data[i+1:]...)
			return deletedValue, nil
		}
	}
	return nil, errors.New("key doesn't exist")
}

func (mem *memDB) Get(key []byte) ([]byte, error) {
    mem.mu.Lock()
    defer mem.mu.Unlock()

    // Check if the key exists in the in-memory data
    for _, kv := range mem.data {
        if string(kv.Key) == string(key) {
            return kv.Value, nil
        }
    }

    // Key not found in in-memory data, attempt to load from SST file if not already loaded
    if !mem.sstFileLoaded {
        fileName := fmt.Sprintf("file_%d.sst", time.Now().Unix()) 
        err := mem.loadSSTFile(fileName)
        if err != nil {
            return nil, err
        }
    }

    // Search the loaded SST file data for the key
    for _, kv := range mem.data {
        if string(kv.Key) == string(key) {
            return kv.Value, nil
        }
    }

    // Key not found in SST file data either
    return nil, errors.New("key not found")
}

func (mem *memDB) GetAll() ([]KeyValue, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	return mem.data, nil
}

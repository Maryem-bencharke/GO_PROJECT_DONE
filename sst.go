package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key       []byte    `json:"Key"`
	Value     []byte    `json:"Value"`
	Operation Operation `json:"Operation"`
}

func (mem *memDB) periodicFlush() {
	ticker := time.NewTicker(30 * time.Minute) // Adjust the duration
	defer ticker.Stop()

	for range ticker.C {
		mem.flushToSST(Set)    // Flush Set operation data
		mem.flushToSST(Delete) // Flush Delete operation data
	}
}

const (
	magicNumber    uint32 = 0x12345678
	version        uint16 = 1
	checksumOffset        = 14 // Offset for checksum in the file
)

func (mem *memDB) createSSTFile() error {
	if len(mem.data) == 0 {
		fmt.Println("No data to create SST file")
		return nil
	}

	// Sort the data before flushing
	sort.Slice(mem.data, func(i, j int) bool {
		return string(mem.data[i].Key) < string(mem.data[j].Key)
	})

	fileName := fmt.Sprintf("file_%d.sst", time.Now().Unix())
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("error creating SST file: %w", err)
	}
	defer file.Close()
	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	entryCount := uint32(len(mem.data))
	smallestKey := mem.data[0].Key
	largestKey := mem.data[len(mem.data)-1].Key

	if err := binary.Write(file, binary.LittleEndian, magicNumber); err != nil {
		return fmt.Errorf("error writing magic number: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, version); err != nil {
		return fmt.Errorf("error writing version: %w", err)
	}

	if err := binary.Write(file, binary.LittleEndian, entryCount); err != nil {
		return fmt.Errorf("error writing entry count: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(smallestKey))); err != nil {
		return fmt.Errorf("error writing smallest key length: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(largestKey))); err != nil {
		return fmt.Errorf("error writing largest key length: %w", err)
	}
	placeholder := uint32(0)
	if err := binary.Write(file, binary.LittleEndian, placeholder); err != nil {
		return fmt.Errorf("error writing entry count placeholder: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, placeholder); err != nil {
		return fmt.Errorf("error writing smallest key length placeholder: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, placeholder); err != nil {
		return fmt.Errorf("error writing largest key length placeholder: %w", err)
	}

	for _, kv := range mem.data {
		if err := binary.Write(file, binary.LittleEndian, uint32(len(kv.Key))); err != nil {
			return fmt.Errorf("error writing key length: %w", err)
		}
		if _, err := file.Write(kv.Key); err != nil {
			return fmt.Errorf("error writing key data: %w", err)
		}
		if err := binary.Write(file, binary.LittleEndian, uint32(len(kv.Value))); err != nil {
			return fmt.Errorf("error writing value length: %w", err)
		}
		if _, err := file.Write(kv.Value); err != nil {
			return fmt.Errorf("error writing value data: %w", err)
		}
	}
	if _, err := file.Seek(checksumOffset, io.SeekStart); err != nil {
		return fmt.Errorf("error seeking to checksum offset: %w", err)
	}
	checksum := calculateChecksum(mem.data)
	if err := binary.Write(file, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("error writing checksum: %w", err)
	}

	mem.data = make([]KeyValue, 0)

	fmt.Println("SST file created successfully:", fileName)
	return nil
}

func (mem *memDB) flushToSST(operation Operation) error {
	var dataToFlush []KeyValue

	switch operation {
	case Set:
		dataToFlush = mem.setData
	case Delete:
		dataToFlush = mem.deleteData
	default:
		return errors.New("invalid operation")
	}

	if len(dataToFlush) == 0 {
		// Handle the case of an empty slice gracefully
		fmt.Println("No data to flush to SST file")
		return nil
	}
	// Sort the data before flushing
	sort.Slice(dataToFlush, func(i, j int) bool {
		return string(dataToFlush[i].Key) < string(dataToFlush[j].Key)
	})

	fileName := fmt.Sprintf("file%d.sst", time.Now().Unix())
	file, err := os.Create(fileName)
	
	if err != nil {
		return err
	}
	defer file.Close()

	entryCount := uint32(len(mem.data))
	smallestKey := mem.data[0].Key
	largestKey := mem.data[len(mem.data)-1].Key

	// Writing magic number and version to the file
	if err := binary.Write(file, binary.LittleEndian, magicNumber); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, version); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, magicNumber); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, version); err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, entryCount); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(smallestKey))); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(largestKey))); err != nil {
		return err
	}

	for _, kv := range mem.data {
		kv.Operation = operation

		if err := binary.Write(file, binary.LittleEndian, uint32(len(kv.Key))); err != nil {
			return err
		}
		if _, err := file.Write(kv.Key); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, uint32(len(kv.Value))); err != nil {
			return err
		}
		if _, err := file.Write(kv.Value); err != nil {
			return err
		}
	}

	if len(mem.data) >= maxEntriesBeforeSST {
		if err := mem.createSSTFile(); err != nil {
			return err
		}
	}
	// Calculate a simple checksum (for demonstration purposes)
	checksum := calculateChecksum(mem.data)
	if err := binary.Write(file, binary.LittleEndian, checksum); err != nil {
		return err
	}

	// Clear memtable after flushing to SST file

	if operation == Set {
		mem.setData = nil
	} else if operation == Delete {
		mem.deleteData = nil
	}
	// Update the watermark position in the WAL
	currentPosition, err := mem.wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	mem.wal.UpdateWatermark(currentPosition)
	fmt.Println("SST file created successfully:", fileName)

	return nil
}

// Calculate a simple checksum (for demonstration purposes)

func calculateChecksum(data []KeyValue) uint32 {
	hash := crc32.NewIEEE()

	for _, kv := range data {
		hash.Write(kv.Key)
		hash.Write(kv.Value)
	}

	return hash.Sum32()
}
func mergeSSTFiles(fileNames []string, newFileName string) error {
	// Open the new file for writing merged data
	newFile, err := os.Create(newFileName)
	if err != nil {
		return err
	}
	defer newFile.Close()

	mergedData := make(map[string]string) // Map to hold merged key-value pairs

	// Iterate through each smaller SST file
	for _, fileName := range fileNames {
		// Open the smaller SST file
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)

		// Process each line in the SST file
		for scanner.Scan() {
			line := scanner.Text()

			var keyValue KeyValue
			if err := json.Unmarshal([]byte(line), &keyValue); err != nil {
				return err // Handle parsing error
			}

			// For simplicity, it just updates or appends keys in mergedData
			mergedData[string(keyValue.Key)] = string(keyValue.Value)
		}

		if err := scanner.Err(); err != nil {
			return err // Handle scanner error
		}

		// Remove the smaller file after merging 
		if err := os.Remove(fileName); err != nil {
			return err
		}
	}

	// Write the merged key-value pairs to the new larger SST file
	for key, value := range mergedData {
		// Convert key and value to bytes
		keyBytes := []byte(key)
		valueBytes := []byte(value)

		// Write key length to file
		keyLen := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyLen, uint32(len(keyBytes)))
		if _, err := newFile.Write(keyLen); err != nil {
			return err
		}

		// Write key to file
		if _, err := newFile.Write(keyBytes); err != nil {
			return err
		}

		// Write value length to file
		valueLen := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueLen, uint32(len(valueBytes)))
		if _, err := newFile.Write(valueLen); err != nil {
			return err
		}

		// Write value to file
		if _, err := newFile.Write(valueBytes); err != nil {
			return err
		}
	}
	return nil
}

func compactSSTFiles(maxSSTFiles int) error {
	sstFiles, err := getSSTFileNames()
	if err != nil {
		return fmt.Errorf("error getting SST file names: %w", err)
	}

	if len(sstFiles) <= maxSSTFiles {
		return nil // No need for compaction, files count within limits
	}

	// Sort SST file names to ensure the order
	sort.Strings(sstFiles)

	// Merge smaller SST files into a larger one
	newSSTFileName := fmt.Sprintf("merged_sst_file_%d.sst", time.Now().Unix()) // Change the filename as needed
	err = mergeSSTFiles(sstFiles, newSSTFileName)
	if err != nil {
		return fmt.Errorf("error during compaction: %w", err)
	}

	// Remove the smaller SST files after successful compaction
	for _, fileName := range sstFiles {
		if err := os.Remove(fileName); err != nil {
			return fmt.Errorf("error removing SST file: %w", err)
		}
	}

	return nil
}

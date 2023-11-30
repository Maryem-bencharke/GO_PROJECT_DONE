package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Operation uint8

const (
	Set Operation = iota
	Delete
)

type WriteAheadLog struct {
	file      *os.File // File to save the log
	watermark int64
}

func NewWriteAheadLog(filePath string) (*WriteAheadLog, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WriteAheadLog{
		file: file,
	}, nil
}

func (wal *WriteAheadLog) AppendEntry(operation Operation, entry KeyValue) error {
	opByte := uint8(operation)
	keyLen := uint16(len(entry.Key))
	valueLen := uint16(len(entry.Value))

	if err := binary.Write(wal.file, binary.LittleEndian, opByte); err != nil {
		return err
	}
	if err := binary.Write(wal.file, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if _, err := wal.file.Write(entry.Key); err != nil {
		return err
	}
	if err := binary.Write(wal.file, binary.LittleEndian, valueLen); err != nil {
		return err
	}
	if _, err := wal.file.Write(entry.Value); err != nil {
		return err
	}

	return nil
}

func (wal *WriteAheadLog) Close() error {
	return wal.file.Close()
}

func (wal *WriteAheadLog) CleanupAfterSSTCreation(position int64) error {
	if wal.file == nil {
		return fmt.Errorf("WAL file not initialized")
	}

	// Close the file handle before truncating
	if err := wal.file.Close(); err != nil {
		return fmt.Errorf("error closing WAL file: %s", err)
	}

	file, err := os.OpenFile(wal.file.Name(), os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error reopening WAL file: %s", err)
	}
	defer file.Close() // Defer closure of the reopened file

	err = file.Truncate(position)
	if err != nil {
		return fmt.Errorf("error truncating WAL file: %s", err)
	}

	wal.file = file // Update the WAL file handle to the reopened file
	_, err = wal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("error seeking end of WAL file: %s", err)
	}

	return nil
}

func (wal *WriteAheadLog) UpdateWatermark(position int64) {
	wal.watermark = position
}

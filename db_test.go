package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestBasicOperations(t *testing.T) {
	wal, err := NewWriteAheadLog("test_wal.log")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	db := NewMemDB(wal)

	key := []byte("test_key")
	value := []byte("test_value")

	// Test Set operation
	if err := db.Set(key, value); err != nil {
		t.Errorf("Set operation failed: %s", err)
	}

	// Test Get operation
	result, err := db.Get(key)
	if err != nil {
		t.Errorf("Get operation failed: %s", err)
	}
	if string(result) != string(value) {
		t.Errorf("Get operation returned incorrect value. Expected: %s, Got: %s", value, result)
	}

	// Test Del operation
	deletedValue, err := db.Del(key)
	if err != nil {
		t.Errorf("Del operation failed: %s", err)
	}
	if string(deletedValue) != string(value) {
		t.Errorf("Del operation returned incorrect deleted value. Expected: %s, Got: %s", value, deletedValue)
	}

	// Test Get after deletion
	_, err = db.Get(key)
	if err == nil {
		t.Error("Get after deletion should return an error, but it didn't")
	}
}

func TestPerformance(t *testing.T) {
	wal, err := NewWriteAheadLog("test_wal.log")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	db := NewMemDB(wal)

	start := time.Now()

	// Test performance with large datasets
	// Example: Insert a large number of entries
	numEntries := 10 // Number of entries to insert
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Error inserting entry: %v", err)
		}
	}

	elapsed := time.Since(start)
	t.Logf("Inserted %d entries in %s", numEntries, elapsed)
}

func TestParameterTuning(t *testing.T) {
	wal, err := NewWriteAheadLog("test_wal2.log")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	db := NewMemDB(wal)
	// Record the start time
	startTime := time.Now()
	// Modify the flushing interval and observe its impact on performance or file sizes
	originalInterval := 5 * time.Minute
	modifiedInterval := 2 * time.Minute

	// Set a modified flushing interval
	db.SetFlushInterval(modifiedInterval)

	// Perform operations that would trigger flushing (e.g., inserting entries)
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Error inserting entry: %v", err)
		}
	}

	endTime := time.Now()

	// Calculate and log the elapsed time
	elapsedTime := endTime.Sub(startTime)
	t.Logf("Elapsed time for inserting %d entries: %s", numEntries, elapsedTime)
	// Reset the flushing interval to its original value for consistency
	db.SetFlushInterval(originalInterval)
}

func TestMemDB_CreateSSTFile(t *testing.T) {
	mem := &memDB{
		data: []KeyValue{
			{Key: []byte("key3"), Value: []byte("value3")},
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}

	if err := mem.createSSTFile(); err != nil {
		t.Errorf("Error creating SST file: %s", err)
	}

	// Check if the SST file is created
	fileName := fmt.Sprintf("file_%d.sst", time.Now().Unix())
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		t.Errorf("SST file not created: %s", err)
	}
}

func TestCreateAndFlushSSTFile(t *testing.T) {
	// Initialize memDB
	mem := &memDB{
		data: []KeyValue{
			{Key: []byte("key3"), Value: []byte("value3")},
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}

	moreData := []KeyValue{
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}

	// Append the new data to the existing memDB data
	mem.data = append(mem.data, moreData...)

	// Call createSSTFile and flushToSST within the same test function
	if err := mem.createSSTFile(); err != nil {
		t.Errorf("Error creating SST file: %s", err)
		return
	}

	if err := mem.flushToSST(Set); err != nil {
		t.Errorf("Error flushing to SST file: %s", err)
		return
	}
	if err := mem.flushToSST(Delete); err != nil {
		t.Errorf("Error flushing to SST file: %s", err)
		return
	}
}

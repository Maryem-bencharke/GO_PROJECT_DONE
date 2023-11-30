package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const maxEntriesBeforeSST = 1000 // Define the threshold
const maxSSTFiles = 10

func main() {
	// Create a WriteAheadLog
	wal, err := NewWriteAheadLog("newal.log")
	watermarkPosition := int64(50)
	if err != nil {
		log.Fatal(err)
	}
	defer wal.Close()

	// Create a memDB instance with the WriteAheadLog
	db := NewMemDB(wal)
	go db.periodicFlush()

	// Create a WaitGroup for handling graceful shutdown
	var wg sync.WaitGroup
	wg.Add(1)

	// Set up HTTP server with graceful shutdown
	server := &http.Server{
		Addr: ":8080",
	}

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		if key == "" || value == "" {
			http.Error(w, "Both key and value are required", http.StatusBadRequest)
			return
		}

		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Println("Set endpoint called with key:", key, "and value:", value)
	})

	http.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		deletedValue, err := db.Del([]byte(key))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		response, _ := json.Marshal(map[string]string{"key": string(key), "deleted_value": string(deletedValue)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(response)
		fmt.Println("DEL endpoint called with key:", key, "and value:", string(deletedValue))
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		value, err := db.Get([]byte(key))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		response, _ := json.Marshal(map[string]string{"key": string(key), "value": string(value)})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(response)
		fmt.Println("Get endpoint called with key:", key, "and value:", string(value))
	})

	// Graceful shutdown handler
	http.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		wg.Done() // Signal the WaitGroup to finish the server gracefully
	})

	fmt.Println("Server running on port 8080")
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %s\n", err)
		}
	}()

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			sstFiles, err := getSSTFileNames()
			if err != nil {
				log.Fatalf("Error getting SST file names: %s\n", err)
			}

			if len(sstFiles) >= maxSSTFiles {
				fileNames, err := getSSTFileNames()
				if err != nil {
					log.Fatalf("Error getting SST file names: %s\n", err)
				}

				for _, fileName := range fileNames {
					if err := os.Remove(fileName); err != nil {
						log.Printf("Error removing SST file: %s\n", err)
					}
				}
			}

			log.Println("Performing additional periodic checks or tasks...")
		}
	}()

	go func() {
		ticker := time.NewTicker(30 * time.Minute) // Adjust the duration as needed
		defer ticker.Stop()

		for range ticker.C {
			err := compactSSTFiles(maxSSTFiles)
			if err != nil {
				log.Fatalf("error during compaction: %s\n", err)
			}

			log.Println("Compaction process completed.")
		}
	}()
	// Wait for graceful shutdown signal
	wg.Wait()

	// Shutdown server gracefully
	fmt.Println("Shutting down the server...")

	// Flush remaining data to SST file before exit
	fmt.Println("Flushing remaining data to SST file before exit...")
	if err := db.createSSTFile(); err != nil {
		log.Fatalf("Error creating SST file: %s\n", err)
	}
	// Trigger cleanup after SST creation
	err = wal.CleanupAfterSSTCreation(watermarkPosition)
	if err != nil {
		fmt.Println("Error cleaning up WAL:", err)
		return
	}
	fmt.Println("WAL cleaned up successfully up to position", watermarkPosition)
	fmt.Println("Server gracefully stopped.")
}
func getSSTFileNames() ([]string, error) {
	dir := "./GO_PROJECT" 

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var sstFileNames []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sst") {
			sstFileNames = append(sstFileNames, file.Name())
		}
	}

	return sstFileNames, nil
}

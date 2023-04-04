package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultTimeoutSeconds = 10 // Default blocking queue read timeout in seconds
)

type Datastore struct {
	mu   sync.Mutex
	data map[string]*Data
}

type Data struct {
	value    string
	expiry   time.Time
	isQueued bool
	queue    []string
}

func NewDatastore() *Datastore {
	return &Datastore{data: make(map[string]*Data)}
}

func (ds *Datastore) Set(key, value string, expirySeconds int, conditional string) (string, int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if _, ok := ds.data[key]; ok {
		if conditional == "NX" { // If key already exists and NX flag is set, do not set value
			return "", http.StatusConflict
		}
	} else if conditional == "XX" { // If key does not exist and XX flag is set, do not set value
		return "", http.StatusNotFound
	}

	var expiry time.Time
	if expirySeconds > 0 {
		expiry = time.Now().Add(time.Duration(expirySeconds) * time.Second)
	}

	ds.data[key] = &Data{value: value, expiry: expiry, isQueued: false}

	return "Enter data sucessfull", http.StatusOK
}

func (ds *Datastore) Get(key string) (string, int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if data, ok := ds.data[key]; ok {
		if data.expiry.IsZero() || time.Now().Before(data.expiry) {
			return data.value, http.StatusOK
		}
	}

	return "Key not exist", http.StatusNotFound
}

func (ds *Datastore) QPush(key string, values ...string) (string, int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	data := ds.data[key]
	if data == nil {
		data = &Data{isQueued: true, queue: []string{}}
		ds.data[key] = data
	} else if !data.isQueued {
		// Key exists but is not a queue
		return "Key already exists", http.StatusConflict
	}

	data.queue = append(data.queue, values...)

	return "Value is pushed successfully", http.StatusOK
}

func (ds *Datastore) QPop(key string) (string, int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	data := ds.data[key]
	if data == nil || !data.isQueued || len(data.queue) == 0 {
		return "Q is empty so nothing can be popped!!", http.StatusBadRequest
	}

	value := data.queue[len(data.queue)-1]
	data.queue = data.queue[:len(data.queue)-1]

	return value, http.StatusOK
}

func (ds *Datastore) BQPop(key string, timeoutSeconds float64) (string, int) {
	timeout := time.Duration(time.Second * time.Duration(timeoutSeconds))
	expiry := time.Now().Add(timeout)

	for {
		ds.mu.Lock()
		data := ds.data[key]
		if data == nil || !data.isQueued || len(data.queue) == 0 {
			// Queue is empty
			ds.mu.Unlock()

			if time.Now().After(expiry) {
				// Timeout expired
				return "", http.StatusNotFound
			}

			time.Sleep(100 * time.Millisecond) // Wait before trying again
			continue
		}

		value := data.queue[len(data.queue)-1]
		data.queue = data.queue[:len(data.queue)-1]

		ds.mu.Unlock()
		return value, http.StatusOK
	}
}




func (ds *Datastore) ValidateSetInput(args []string) bool {
	if len(args) < 2 {
		return false
	}

	if len(args) > 5 {
		return false
	}

	if len(args) >= 4 && (!strings.HasPrefix(args[2], "EX") || len(args[2]) <= 2) {
		return false
	}

	if len(args) == 5 && (args[4] != "NX" && args[4] != "XX") {
		return false
	}

	return true
}

func (ds *Datastore) ValidateBQPopInput(args []string) bool {
	if len(args) != 2 {
		return false
	}

	if _, err := strconv.ParseFloat(args[1], 64); err != nil {
		return false
	}

	return true
}

func (ds *Datastore) ParseCommand(rawCommand string) (string, []string) {
	args := strings.Split(rawCommand, " ")
	command := strings.ToUpper(args[0])
	args = args[1:]

	return command, args
}

func (ds *Datastore) HandleCommand(rawCommand string) (interface{}, int) {
	command, args := ds.ParseCommand(rawCommand)

	switch command {
	case "SET":
		if !ds.ValidateSetInput(args) {
			return "Invalid Command", http.StatusBadRequest
		}
		key := args[0]
		value := args[1]
		var expirySeconds int
		if len(args) >= 3 {
			expirySeconds, _ = strconv.Atoi(args[2][2:])
		}
		conditional := ""
		if len(args) == 5 {
			conditional = args[4]
		}
		return ds.Set(key, value, expirySeconds, conditional)

	case "GET":
		if len(args) != 1 {
			return "Invalid Command", http.StatusBadRequest
		}
		key := args[0];	value, status := ds.Get(key)
		if status == http.StatusOK {
			return map[string]string{"value": value}, status
		}
		return value, status

	case "QPUSH":
		if len(args) < 2 {
			return nil, http.StatusBadRequest
		}
		key := args[0]
		values := args[1:]
		return ds.QPush(key, values...)

	case "QPOP":
		if len(args) != 1 {
			return nil, http.StatusBadRequest
		}
		key := args[0]
		value, status := ds.QPop(key)
		if status == http.StatusOK {
			return map[string]string{"value": value}, status
		}
		
			return map[string]string{"error": value}, status
	case "BQPOP":
		if !ds.ValidateBQPopInput(args) {
			return nil, http.StatusBadRequest
		}
		key := args[0]
		timeoutSeconds, _ := strconv.ParseFloat(args[1], 64)
		value, status := ds.BQPop(key, timeoutSeconds)
		if status == http.StatusOK {
			return map[string]string{"value": value}, status
		}
		return nil, status

	default:
		return "Invalid Command", http.StatusBadRequest
	}
}

func main() {
	datastore := NewDatastore()

	http.HandleFunc("/command/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		contentType := r.Header.Get("Content-Type")
		if contentType != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var jsonRequest struct {
			Command string `json:"command"`
		}
		err := json.NewDecoder(r.Body).Decode(&jsonRequest)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result, status := datastore.HandleCommand(jsonRequest.Command)

		if status == http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(result)
		} else {
            w.Header().Set("Content-Type", "application/json")
		    w.WriteHeader(status)
		    json.NewEncoder(w).Encode(result)
		}
	})

	fmt.Println("Starting server on port 8080...")
	http.ListenAndServe(":8080", nil)
}

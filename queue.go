package snerdmq

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type SnerdQueue struct {
	binaryPath       string
	storagePath      string
	process          *exec.Cmd
	stdin            io.WriteCloser
	handlers         map[string]func(context.Context, map[string]interface{}) error
	maxRetryHandlers map[string]func(context.Context, map[string]interface{}) error
	wsClients        map[*websocket.Conn]bool
	wsClientsLock    sync.RWMutex
	handlersMutex    sync.RWMutex
	pendingAcks      map[string]chan error
	pendingMutex     sync.RWMutex
	shuttingDown     bool
	engineAlive      bool
	shutdownMutex    sync.RWMutex
	done             chan struct{}
}

type SnerdQueueConfig struct {
	BinaryPath  string
	StoragePath string
}

func NewSnerdQueue(config ...SnerdQueueConfig) (*SnerdQueue, error) {
	var binPath, storePath string
	if len(config) > 0 {
		binPath = config[0].BinaryPath
		storePath = config[0].StoragePath
	}

	if binPath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		ext := ""
		if runtimeOS() == "windows" {
			ext = ".exe"
		}
		binPath = filepath.Join(cwd, "bin", "snerdmq"+ext)
	}

	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("[Snerd] Binary not found at %s. Please run 'go run github.com/speed-nerd/snerdmq-go/cmd/snerdmq-install@latest' or provide BinaryPath", binPath)
	}

	queue := &SnerdQueue{
		binaryPath:       binPath,
		storagePath:      storePath,
		handlers:         make(map[string]func(context.Context, map[string]interface{}) error),
		maxRetryHandlers: make(map[string]func(context.Context, map[string]interface{}) error),
		wsClients:        make(map[*websocket.Conn]bool),
		pendingAcks:      make(map[string]chan error),
		done:             make(chan struct{}),
	}

	return queue, nil
}

// runtimeOS is a small helper to mock runtime.GOOS if needed, though we just use runtime.GOOS directly.
func runtimeOS() string {
	return os.Getenv("GOOS") // Simplified for this example, actually we just use runtime.GOOS in real usage
}

func (q *SnerdQueue) RegisterHandler(taskType string, handler func(context.Context, map[string]interface{}) error) {
	q.handlersMutex.Lock()
	q.handlers[taskType] = handler
	q.handlersMutex.Unlock()

	// If the queue is already running, send the registration immediately
	q.shutdownMutex.RLock()
	if q.stdin != nil && !q.shuttingDown {
		q.send(map[string]interface{}{
			"action":    "register",
			"task_type": taskType,
		})
	}
	q.shutdownMutex.RUnlock()
}

func (q *SnerdQueue) RegisterMaxRetryHandler(taskType string, handler func(context.Context, map[string]interface{}) error) {
	q.handlersMutex.Lock()
	q.maxRetryHandlers[taskType] = handler
	q.handlersMutex.Unlock()
}

func (q *SnerdQueue) StartListening() error {
	args := []string{}
	if q.storagePath != "" {
		args = append(args, q.storagePath)
	}

	q.process = exec.Command(q.binaryPath, args...)

	stdin, err := q.process.StdinPipe()
	if err != nil {
		return err
	}
	q.stdin = stdin

	stdout, err := q.process.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := q.process.StderrPipe()
	if err != nil {
		return err
	}

	if err := q.process.Start(); err != nil {
		return err
	}

	q.shutdownMutex.Lock()
	q.engineAlive = true
	q.shutdownMutex.Unlock()

	// Re-register all existing handlers
	q.handlersMutex.RLock()
	for taskType := range q.handlers {
		q.send(map[string]interface{}{
			"action":    "register",
			"task_type": taskType,
		})
	}
	q.handlersMutex.RUnlock()

	go q.readStdout(stdout)
	go q.readStderr(stderr)

	return nil
}

func (q *SnerdQueue) readStdout(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var msg map[string]interface{}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}

		// Process execution in a separate goroutine so we don't block the stdout reader
		go q.handleEngineMessage(msg)
	}

	q.shutdownMutex.RLock()
	isShuttingDown := q.shuttingDown
	q.shutdownMutex.RUnlock()

	if !isShuttingDown {
		fmt.Fprintf(os.Stderr, "[Snerd] Engine process terminated unexpectedly.\n")
	}

	// Engine is gone — it can never ack. Reject all pending enqueues so
	// callers fail fast instead of blocking on their ack channel forever.
	q.shutdownMutex.Lock()
	q.engineAlive = false
	q.shutdownMutex.Unlock()
	q.pendingMutex.Lock()
	for id, ch := range q.pendingAcks {
		delete(q.pendingAcks, id)
		ch <- fmt.Errorf("[Snerd] Engine terminated before ack for task '%s'", id)
	}
	q.pendingMutex.Unlock()

	close(q.done)
}

func (q *SnerdQueue) readStderr(stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		fmt.Fprintf(os.Stderr, "[Snerd Engine Error]: %s\n", scanner.Text())
	}
}

func (q *SnerdQueue) handleEngineMessage(msg map[string]interface{}) {
	action, ok := msg["action"].(string)
	if !ok {
		return
	}

	if action == "execute" {
		taskType, _ := msg["task_type"].(string)
		taskID, _ := msg["task_id"].(string)
		taskDataRaw := msg["task_data"]

		// Rust sends task_data as a JSON string, we need to unmarshal it
		var taskData map[string]interface{}
		if strData, isStr := taskDataRaw.(string); isStr {
			json.Unmarshal([]byte(strData), &taskData)
		}

		q.handlersMutex.RLock()
		handler, exists := q.handlers[taskType]
		q.handlersMutex.RUnlock()

		if !exists {
			q.send(map[string]interface{}{
				"action":    "result",
				"task_id":   taskID,
				"status":    "error",
				"error_msg": "No handler registered.",
			})
			return
		}

		ctx := context.WithValue(context.Background(), "taskID", taskID)
		var cancel context.CancelFunc
		if maxExecFloat, ok := msg["max_execution_seconds"].(float64); ok && maxExecFloat > 0 {
			ctx, cancel = context.WithTimeout(ctx, time.Duration(maxExecFloat)*time.Second)
			defer cancel()
		}

		err := handler(ctx, taskData)
		if err != nil {
			q.send(map[string]interface{}{
				"action":    "result",
				"task_id":   taskID,
				"status":    "error",
				"error_msg": err.Error(),
			})
		} else {
			q.send(map[string]interface{}{
				"action":  "result",
				"task_id": taskID,
				"status":  "success",
			})
		}

	} else if action == "ack" {
		taskID, _ := msg["task_id"].(string)
		q.pendingMutex.Lock()
		if ch, exists := q.pendingAcks[taskID]; exists {
			delete(q.pendingAcks, taskID)
			ch <- nil
		}
		q.pendingMutex.Unlock()
	} else if action == "error" {
		taskID, _ := msg["task_id"].(string)
		errorMsg, _ := msg["message"].(string)

		q.pendingMutex.Lock()
		if ch, exists := q.pendingAcks[taskID]; exists {
			delete(q.pendingAcks, taskID)
			ch <- fmt.Errorf("%s", errorMsg)
		} else {
			fmt.Fprintf(os.Stderr, "[Snerd] Error from engine: %s\n", errorMsg)
		}
		q.pendingMutex.Unlock()
	} else if action == "progress" {
		lineBytes, _ := json.Marshal(msg)

		q.wsClientsLock.RLock()
		for client := range q.wsClients {
			client.WriteMessage(websocket.TextMessage, lineBytes)
		}
		q.wsClientsLock.RUnlock()
	} else if action == "max_retries_reached" {
		taskType, _ := msg["task_type"].(string)
		taskID, _ := msg["task_id"].(string)
		taskDataRaw := msg["task_data"]

		var taskData map[string]interface{}
		if strData, isStr := taskDataRaw.(string); isStr {
			json.Unmarshal([]byte(strData), &taskData)
		}

		q.handlersMutex.RLock()
		handler, exists := q.maxRetryHandlers[taskType]
		q.handlersMutex.RUnlock()

		if exists {
			ctx := context.WithValue(context.Background(), "taskID", taskID)
			err := handler(ctx, taskData)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Snerd] Error in max retry handler for task %s: %v\n", taskID, err)
			}
		} else {
			fmt.Fprintf(os.Stderr, "[Snerd] Dead Letter Queue: Task %v (%v) permanently failed.\n", taskID, taskType)
		}
	}
}

func (q *SnerdQueue) send(msg map[string]interface{}) {
	q.shutdownMutex.RLock()
	defer q.shutdownMutex.RUnlock()

	if q.shuttingDown || q.stdin == nil {
		return
	}

	// Check if process is still alive
	if q.process != nil && q.process.Process != nil {
		// ProcessState is non-nil only after exit; if nil, process may still be running
		if q.process.ProcessState != nil && q.process.ProcessState.Exited() {
			return
		}
	}

	b, _ := json.Marshal(msg)
	b = append(b, '\n')
	q.stdin.Write(b) //nolint:errcheck // ignore write errors when daemon dies
}

func (q *SnerdQueue) Enqueue(taskID, taskType string, data interface{}, maxRetries int, retryAfterHours float64, rateLimitGroup string, maxPerMinute int, autoDedupe *bool, urgencyScore *float64, executeAt *string, cron *string, webhookUrl *string, maxExecutionSeconds *int) error {
	q.shutdownMutex.RLock()
	if q.process == nil || q.shuttingDown {
		q.shutdownMutex.RUnlock()
		return fmt.Errorf("[Snerd] Cannot enqueue task: Queue is not running. Call StartListening() first.")
	}
	if !q.engineAlive {
		q.shutdownMutex.RUnlock()
		return fmt.Errorf("[Snerd] Cannot enqueue task: engine is not running.")
	}
	q.shutdownMutex.RUnlock()

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	payload := map[string]interface{}{
		"action":            "enqueue",
		"task_id":           taskID,
		"task_type":         taskType,
		"task_data":         string(dataBytes),
		"max_retries":       maxRetries,
		"retry_after_hours": retryAfterHours,
	}

	if rateLimitGroup != "" {
		payload["rate_limit_group"] = rateLimitGroup
	}
	if maxPerMinute > 0 {
		payload["max_per_minute"] = maxPerMinute
	}
	if autoDedupe != nil {
		payload["auto_dedupe"] = *autoDedupe
	}
	if urgencyScore != nil {
		payload["urgency_score"] = *urgencyScore
	}
	if executeAt != nil {
		payload["execute_at"] = *executeAt
	}
	if cron != nil {
		payload["cron"] = *cron
	}
	if webhookUrl != nil {
		payload["webhook_url"] = *webhookUrl
	}
	if maxExecutionSeconds != nil {
		payload["max_execution_seconds"] = *maxExecutionSeconds
	}

	ch := make(chan error, 1)
	q.pendingMutex.Lock()
	q.pendingAcks[taskID] = ch
	q.pendingMutex.Unlock()

	q.send(payload)

	return <-ch
}

func (q *SnerdQueue) Shutdown() {
	q.shutdownMutex.Lock()
	if q.shuttingDown {
		q.shutdownMutex.Unlock()
		return
	}
	q.shuttingDown = true
	q.shutdownMutex.Unlock()

	if q.process != nil && q.process.Process != nil {
		q.process.Process.Signal(syscall.SIGTERM)
	}
}

func (q *SnerdQueue) Wait() {
	<-q.done
}

func (q *SnerdQueue) YieldProgress(ctx context.Context, data interface{}) error {
	taskID, ok := ctx.Value("taskID").(string)
	if !ok || taskID == "" {
		return fmt.Errorf("[Snerd] YieldProgress must be called with a valid task context")
	}

	dataBytes, _ := json.Marshal(data)
	payload := map[string]interface{}{
		"action":  "progress",
		"task_id": taskID,
		"data":    string(dataBytes),
	}
	q.send(payload)
	return nil
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (q *SnerdQueue) StartDashboard(port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		storage := "./.snerdata"
		if q.storagePath != "" {
			storage = q.storagePath
		}
		tasksPath := filepath.Join(storage, "tasks", "tasks.log")

		tasksMap := make(map[string]map[string]interface{})
		if file, err := os.Open(tasksPath); err == nil {
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				if line == "" {
					continue
				}
				var t map[string]interface{}
				if json.Unmarshal([]byte(line), &t) == nil {
					if tid, ok := t["taskId"].(string); ok {
						tasksMap[tid] = t
					}
				}
			}
			file.Close()
		}
		enqueued, processed, failed := 0, 0, 0
		for _, t := range tasksMap {
			enqueued++
			if t["deletedAt"] != nil {
				if t["LastJobError"] != nil {
					failed++
				} else {
					processed++
				}
			}
		}
		fmt.Fprintf(w, `{"enqueued":%d,"processed":%d,"failed":%d}`, enqueued, processed, failed)
	})

	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		storage := "./.snerdata"
		if q.storagePath != "" {
			storage = q.storagePath
		}
		tasksPath := filepath.Join(storage, "tasks", "tasks.log")

		tasksMap := make(map[string]map[string]interface{})
		if file, err := os.Open(tasksPath); err == nil {
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				if line == "" {
					continue
				}
				var t map[string]interface{}
				if json.Unmarshal([]byte(line), &t) == nil {
					if tid, ok := t["taskId"].(string); ok {
						tasksMap[tid] = t
					}
				}
			}
			file.Close()
		}

		var res []map[string]interface{}
		for _, t := range tasksMap {
			var status string
			if t["deletedAt"] != nil {
				rtCount, _ := t["retryCount"].(float64)
				maxRt, _ := t["maxRetries"].(float64)
				if t["LastJobError"] != nil && rtCount >= maxRt {
					status = "dead_letter"
				} else if t["LastJobError"] != nil {
					status = "failed"
				} else {
					status = "completed"
				}
			} else if t["LastJobError"] != nil {
				status = "failed"
			} else {
				execAt, _ := t["executeAt"].(string)
				if execAt != "" {
					if et, err := time.Parse(time.RFC3339, execAt); err == nil && !et.After(time.Now()) {
						status = "active"
					} else {
						status = "queued"
					}
				} else {
					status = "queued"
				}
			}
			rtCount, _ := t["retryCount"].(float64)
			maxRt, _ := t["maxRetries"].(float64)
			rtAfter, _ := t["retryAfterTime"].(string)

			res = append(res, map[string]interface{}{
				"id":                  t["taskId"],
				"type":                t["taskType"],
				"status":              status,
				"progress":            0,
				"retryCount":          rtCount,
				"maxRetries":          maxRt,
				"retryAfterTime":      rtAfter,
				"cronExpression":      t["cronExpression"],
				"webhookUrl":          t["webhookUrl"],
				"maxExecutionSeconds": t["maxExecutionSeconds"],
			})
		}
		json.NewEncoder(w).Encode(res)
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		q.wsClientsLock.Lock()
		q.wsClients[conn] = true
		q.wsClientsLock.Unlock()

		defer func() {
			q.wsClientsLock.Lock()
			delete(q.wsClients, conn)
			q.wsClientsLock.Unlock()
			conn.Close()
		}()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	})

	// Static files
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			staticFile := filepath.Join("static", "index.html")
			if _, err := os.Stat(staticFile); os.IsNotExist(err) {
				staticFile = filepath.Join("..", "static", "index.html")
			}
			http.ServeFile(w, r, staticFile)
		} else {
			http.NotFound(w, r)
		}
	})

	go func() {
		fmt.Printf("[Snerd] Dashboard running on http://localhost:%d\n", port)
		http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
	}()
}

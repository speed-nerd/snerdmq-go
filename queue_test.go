package snerdmq

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func wipeTestDB(dbPath string) {
	os.Remove(dbPath)
}

func TestSnerdQueueIntegration(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get cwd: %v", err)
	}

	ext := ""
	if runtimeOS() == "windows" {
		ext = ".exe"
	}

	// Point to the actual compiled Rust binary in the sibling repository for tests
	binPath := filepath.Join(cwd, "..", "snerdmq", "target", "debug", "snerdmq"+ext)
	dbPath := filepath.Join(cwd, "..", ".snerdata", "tasks", "tasks.log")

	wipeTestDB(dbPath)

	queue, err := NewSnerdQueue(SnerdQueueConfig{
		BinaryPath: binPath,
	})
	if err != nil {
		t.Fatalf("Failed to init queue: %v", err)
	}

	jobCompleted := make(chan bool)

	queue.RegisterHandler("test_go_notification", func(ctx context.Context, data map[string]interface{}) error {
		userID := data["user_id"].(string)
		message := data["message"].(string)

		if userID != "john_wick" {
			t.Errorf("Expected john_wick, got %s", userID)
		}
		if message != "Baba Yaga" {
			t.Errorf("Expected Baba Yaga, got %s", message)
		}

		jobCompleted <- true
		return nil
	})

	if err := queue.StartListening(); err != nil {
		t.Fatalf("Failed to start listening: %v", err)
	}

	// Give daemon a tiny fraction of a second to boot up
	time.Sleep(100 * time.Millisecond)

	err = queue.Enqueue(
		"go-job-1",
		"test_go_notification",
		map[string]interface{}{"user_id": "john_wick", "message": "Baba Yaga"},
		3,
		0,
		"",
		0,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to enqueue: %v", err)
	}

	select {
	case <-jobCompleted:
		// Success!
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for job completion")
	}

	queue.Shutdown()
}

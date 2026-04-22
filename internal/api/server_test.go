package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/internal/auth"
	"github.com/Xecutables/Nebula.Conduit/internal/models"
)

type stubTaskService struct {
	mu sync.Mutex

	createPendingTaskFunc func(script string, args []string, env []string, userID int) (*models.Task, error)
	runTaskFunc           func(taskID string) error
	deletePendingTaskFunc func(taskID string) error
}

func (s *stubTaskService) CreateTask(script string, args []string, env []string, userID int) (*models.Task, error) {
	return nil, nil
}

func (s *stubTaskService) CreatePendingTask(script string, args []string, env []string, userID int) (*models.Task, error) {
	if s.createPendingTaskFunc != nil {
		return s.createPendingTaskFunc(script, args, env, userID)
	}
	return &models.Task{ID: "task-1", Script: script, Args: args, Env: env, CreatedBy: userID, Status: models.StatusPending}, nil
}

func (s *stubTaskService) RunTask(taskID string) error {
	if s.runTaskFunc != nil {
		return s.runTaskFunc(taskID)
	}
	return nil
}

func (s *stubTaskService) DeletePendingTask(taskID string) error {
	if s.deletePendingTaskFunc != nil {
		return s.deletePendingTaskFunc(taskID)
	}
	return nil
}

func (s *stubTaskService) GetTask(id string, userID int) (*models.Task, error) {
	return nil, nil
}

func (s *stubTaskService) StopTask(id string, userID int) error {
	return nil
}

func (s *stubTaskService) ListTasks(userID int) ([]*models.Task, error) {
	return nil, nil
}

func TestHandleCreateTask_QueuesPersistedTask(t *testing.T) {
	server := &Server{
		taskService:     &stubTaskService{},
		circuitBreaker:  &CircuitBreaker{maxFailures: 5},
		taskQueue:       &TaskQueue{queue: make(chan *queuedTask, 1), maxRunning: 1, maxQueueSize: 1},
		resourceMonitor: &ResourceMonitor{},
	}

	atomic.StoreInt64(&server.taskQueue.running, server.taskQueue.maxRunning)

	body, err := json.Marshal(models.TaskRequest{
		Script: "queued-script",
		Args:   []string{"one"},
		Env:    []string{"KEY=value"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
	req = req.WithContext(auth.WithUserID(req.Context(), 42))
	rec := httptest.NewRecorder()

	server.handleCreateTask(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", rec.Code)
	}

	var queued models.Task
	if err := json.NewDecoder(rec.Body).Decode(&queued); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if queued.CreatedBy != 42 {
		t.Fatalf("expected queued task owner 42, got %d", queued.CreatedBy)
	}

	select {
	case item := <-server.taskQueue.queue:
		if item.TaskID != queued.ID {
			t.Fatalf("expected queued task id %q, got %q", queued.ID, item.TaskID)
		}
		if item.UserID != 42 {
			t.Fatalf("expected queued user id 42, got %d", item.UserID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected queued task to be enqueued")
	}
}

func TestHandleCreateTask_QueueOverflowDeletesPendingTask(t *testing.T) {
	var deletedTaskID string

	server := &Server{
		taskService: &stubTaskService{
			createPendingTaskFunc: func(script string, args []string, env []string, userID int) (*models.Task, error) {
				return &models.Task{
					ID:        "task-overflow",
					Script:    script,
					Args:      args,
					Env:       env,
					CreatedBy: userID,
					Status:    models.StatusPending,
				}, nil
			},
			deletePendingTaskFunc: func(taskID string) error {
				deletedTaskID = taskID
				return nil
			},
		},
		circuitBreaker:  &CircuitBreaker{maxFailures: 5},
		taskQueue:       &TaskQueue{queue: make(chan *queuedTask), maxRunning: 0, maxQueueSize: 0},
		resourceMonitor: &ResourceMonitor{},
	}

	body, err := json.Marshal(models.TaskRequest{Script: "overflow-script"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
	req = req.WithContext(auth.WithUserID(req.Context(), 7))
	rec := httptest.NewRecorder()

	server.handleCreateTask(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", rec.Code)
	}

	if deletedTaskID != "task-overflow" {
		t.Fatalf("expected rejected queued task to be deleted, got %q", deletedTaskID)
	}
}

func TestTaskQueueWorker_WaitsForCapacityWithoutDroppingTask(t *testing.T) {
	runStarted := make(chan string, 1)

	server := &Server{
		taskService: &stubTaskService{
			runTaskFunc: func(taskID string) error {
				runStarted <- taskID
				return nil
			},
		},
		circuitBreaker: &CircuitBreaker{maxFailures: 5},
		taskQueue:      &TaskQueue{queue: make(chan *queuedTask, 1), maxRunning: 1, maxQueueSize: 1},
	}

	atomic.StoreInt64(&server.taskQueue.running, 1)
	server.startTaskQueueWorker()
	defer close(server.taskQueue.queue)

	if err := server.taskQueue.Enqueue(&queuedTask{TaskID: "queued-1", UserID: 9}); err != nil {
		t.Fatalf("enqueue queued task: %v", err)
	}

	select {
	case taskID := <-runStarted:
		t.Fatalf("expected task to wait for capacity, but it started immediately: %s", taskID)
	case <-time.After(150 * time.Millisecond):
	}

	server.taskQueue.DecrementRunning()

	select {
	case taskID := <-runStarted:
		if taskID != "queued-1" {
			t.Fatalf("expected queued task queued-1, got %s", taskID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected queued task to start after capacity was released")
	}
}

func TestResourceMonitor_UpdateResourceUsageUsesRealLimits(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	monitor := &ResourceMonitor{
		maxMemoryPercent: 80,
		maxCPUPercent:    80,
		checkInterval:    5 * time.Second,
		now: func() time.Time {
			return now
		},
		readProcessRSS: func() (uint64, error) {
			return 512 * 1024 * 1024, nil
		},
		readMemoryLimit: func() (uint64, error) {
			return 2 * 1024 * 1024 * 1024, nil
		},
		readProcessCPU: func() (float64, error) {
			return 160, nil
		},
		readCPUCapacity: func() (float64, error) {
			return 4, nil
		},
	}

	monitor.updateResourceUsage()

	memUsage, cpuUsage := monitor.GetStats()
	if memUsage != 25 {
		t.Fatalf("expected memory usage 25, got %v", memUsage)
	}
	if cpuUsage != 40 {
		t.Fatalf("expected cpu usage 40, got %v", cpuUsage)
	}
}

func TestResourceMonitor_IsOverloadedRefreshesStaleStats(t *testing.T) {
	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	callCount := 0

	monitor := &ResourceMonitor{
		maxMemoryPercent: 80,
		maxCPUPercent:    80,
		checkInterval:    5 * time.Second,
		lastMemoryCheck:  now.Add(-10 * time.Second),
		lastCPUCheck:     now.Add(-10 * time.Second),
		now: func() time.Time {
			return now
		},
		readProcessRSS: func() (uint64, error) {
			callCount++
			return 9, nil
		},
		readMemoryLimit: func() (uint64, error) {
			return 10, nil
		},
		readProcessCPU: func() (float64, error) {
			return 10, nil
		},
		readCPUCapacity: func() (float64, error) {
			return 1, nil
		},
	}

	if !monitor.IsOverloaded() {
		t.Fatal("expected resource monitor to report overload after refreshing stats")
	}
	if callCount == 0 {
		t.Fatal("expected IsOverloaded to refresh stale resource stats")
	}
}

type stubAuthService struct {
	validateTokenFunc func(token string) (int, error)
}

func (s *stubAuthService) Register(username, password, email string) error {
	return nil
}

func (s *stubAuthService) Login(username, password string) (string, error) {
	return "", nil
}

func (s *stubAuthService) ValidateToken(token string) (int, error) {
	if s.validateTokenFunc != nil {
		return s.validateTokenFunc(token)
	}
	return 0, nil
}

func TestStandaloneAuthMiddleware_UsesSharedUserIDContextKey(t *testing.T) {
	middleware := auth.AuthMiddleware(&stubAuthService{
		validateTokenFunc: func(token string) (int, error) {
			if token != "token-123" {
				t.Fatalf("expected token token-123, got %q", token)
			}
			return 99, nil
		},
	})

	var gotUserID int
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, ok := auth.UserIDFromContext(r.Context())
		if !ok {
			t.Fatal("expected user id in request context")
		}
		gotUserID = userID
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	req.Header.Set("Authorization", "Bearer token-123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d", rec.Code)
	}
	if gotUserID != 99 {
		t.Fatalf("expected propagated user id 99, got %d", gotUserID)
	}
}

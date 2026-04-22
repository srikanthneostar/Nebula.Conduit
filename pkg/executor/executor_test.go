package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/Xecutables/Nebula.Conduit/internal/models"
)

type memoryTaskRepo struct {
	mu    sync.Mutex
	tasks map[string]*models.Task
}

func newMemoryTaskRepo() *memoryTaskRepo {
	return &memoryTaskRepo{tasks: make(map[string]*models.Task)}
}

func (r *memoryTaskRepo) CreateTask(task *models.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[task.ID] = cloneTask(task)
	return nil
}

func (r *memoryTaskRepo) GetTask(id string) (*models.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	task, ok := r.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return cloneTask(task), nil
}

func (r *memoryTaskRepo) UpdateTask(task *models.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.tasks[task.ID]; !ok {
		return ErrTaskNotFound
	}

	r.tasks[task.ID] = cloneTask(task)
	return nil
}

func (r *memoryTaskRepo) DeleteTask(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.tasks[id]; !ok {
		return ErrTaskNotFound
	}

	delete(r.tasks, id)
	return nil
}

func (r *memoryTaskRepo) ListTasks(userID int) ([]*models.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var tasks []*models.Task
	for _, task := range r.tasks {
		if task.CreatedBy == userID {
			tasks = append(tasks, cloneTask(task))
		}
	}
	return tasks, nil
}

func cloneTask(task *models.Task) *models.Task {
	cloned := *task
	cloned.Args = append([]string(nil), task.Args...)
	cloned.Env = append([]string(nil), task.Env...)
	return &cloned
}

func createPendingTask(t *testing.T, repo *memoryTaskRepo, id, script string, args []string) {
	t.Helper()

	err := repo.CreateTask(&models.Task{
		ID:        id,
		Script:    script,
		Args:      args,
		Status:    models.StatusPending,
		CreatedBy: 1,
		StartedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
}

func writeScript(t *testing.T, dir, name, body string) string {
	t.Helper()

	path := filepath.Join(dir, name+".py")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func waitForTaskStatus(t *testing.T, repo *memoryTaskRepo, taskID string, timeout time.Duration, statuses ...models.TaskStatus) *models.Task {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task, err := repo.GetTask(taskID)
		if err != nil {
			t.Fatalf("get task %s: %v", taskID, err)
		}

		for _, status := range statuses {
			if task.Status == status {
				return task
			}
		}

		time.Sleep(25 * time.Millisecond)
	}

	task, err := repo.GetTask(taskID)
	if err != nil {
		t.Fatalf("get task %s after timeout: %v", taskID, err)
	}
	t.Fatalf("timed out waiting for task %s to reach one of %v; current status=%s", taskID, statuses, task.Status)
	return nil
}

func TestRunTask_TimeoutCancelsAndKillsChildProcess(t *testing.T) {
	scriptDir := t.TempDir()
	markerPath := filepath.Join(scriptDir, "marker.txt")

	writeScript(t, scriptDir, "timeoutchild", `
import pathlib
import sys
import time

time.sleep(0.5)
pathlib.Path(sys.argv[1]).write_text("done")
`)

	repo := newMemoryTaskRepo()
	createPendingTask(t, repo, "task-timeout", "timeoutchild", []string{markerPath})

	executor := NewPythonExecutor(repo, &config.PathConfig{
		PythonScriptsHome: scriptDir,
		AllowedPaths:      []string{scriptDir},
	}, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		executor.RunTask(ctx, "task-timeout", "timeoutchild", []string{markerPath}, nil, 1)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RunTask to return")
	}

	task := waitForTaskStatus(t, repo, "task-timeout", time.Second, models.StatusCancelled)
	if task.ExitCode != -2 {
		t.Fatalf("expected cancelled exit code -2, got %d", task.ExitCode)
	}

	time.Sleep(700 * time.Millisecond)

	if _, err := os.Stat(markerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected timeout child process to be killed before creating marker, stat err=%v", err)
	}
}

func TestRunTask_StopTaskPreservesCancelledStatus(t *testing.T) {
	scriptDir := t.TempDir()

	writeScript(t, scriptDir, "stoppable", `
import time

time.sleep(1.0)
print("finished")
`)

	repo := newMemoryTaskRepo()
	createPendingTask(t, repo, "task-stop", "stoppable", nil)

	executor := NewPythonExecutor(repo, &config.PathConfig{
		PythonScriptsHome: scriptDir,
		AllowedPaths:      []string{scriptDir},
	}, time.Second)

	done := make(chan struct{})
	go func() {
		executor.RunTask(context.Background(), "task-stop", "stoppable", nil, nil, 1)
		close(done)
	}()

	waitForTaskStatus(t, repo, "task-stop", time.Second, models.StatusRunning)

	if err := executor.StopTask("task-stop"); err != nil {
		t.Fatalf("stop task: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stopped task to exit")
	}

	task := waitForTaskStatus(t, repo, "task-stop", time.Second, models.StatusCancelled)
	if task.Status != models.StatusCancelled {
		t.Fatalf("expected final status cancelled, got %s", task.Status)
	}
}

func TestRunTask_SuccessRemainsCompleted(t *testing.T) {
	scriptDir := t.TempDir()

	writeScript(t, scriptDir, "successful", `
print("ok")
`)

	repo := newMemoryTaskRepo()
	createPendingTask(t, repo, "task-success", "successful", nil)

	executor := NewPythonExecutor(repo, &config.PathConfig{
		PythonScriptsHome: scriptDir,
		AllowedPaths:      []string{scriptDir},
	}, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	executor.RunTask(ctx, "task-success", "successful", nil, nil, 1)

	task := waitForTaskStatus(t, repo, "task-success", time.Second, models.StatusCompleted)
	if task.Status != models.StatusCompleted {
		t.Fatalf("expected completed task, got %s", task.Status)
	}
}

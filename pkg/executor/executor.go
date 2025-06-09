package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/Xecutables/Nebula.Conduit/internal/models"
	"github.com/Xecutables/Nebula.Conduit/internal/repository"
	"github.com/Xecutables/Nebula.Conduit/pkg/security"
)

var (
	ErrTaskNotFound = errors.New("task not found")
	ErrInvalidTask  = errors.New("invalid task")
)

type PythonExecutor struct {
	taskRepo       repository.TaskRepository
	runningTasks   sync.Map
	pathConfig     *config.PathConfig
	timeout        time.Duration
	validator      security.ScriptValidator
	pythonCommand  string // Add this field
}

func NewPythonExecutor(repo repository.TaskRepository, cfg *config.PathConfig, timeout time.Duration) *PythonExecutor {
	// Ensure the scripts directory exists
	if err := os.MkdirAll(cfg.PythonScriptsHome, 0755); err != nil {
		log.Fatal().Err(err).Msg("Failed to create Python scripts directory")
	}

	// Determine Python command based on platform or configuration
	pythonCmd := "python"
	if _, err := exec.LookPath("py"); err == nil {
		pythonCmd = "py"
	} else if _, err := exec.LookPath("python3"); err == nil {
		pythonCmd = "python3"
	}

	return &PythonExecutor{
		taskRepo:      repo,
		pathConfig:    cfg,
		timeout:       timeout,
		validator:     security.NewScriptValidator(),
		pythonCommand: pythonCmd,
	}
}

func (e *PythonExecutor) validateScriptPath(scriptName string) (string, error) {
	if err := e.validator.Validate(scriptName, nil); err != nil {
		return "", fmt.Errorf("invalid script name: %w", err)
	}

	for _, basePath := range e.pathConfig.AllowedPaths {
		fullPath := filepath.Join(basePath, scriptName)
		if _, err := os.Stat(fullPath); err == nil {
			return fullPath, nil
		}
	}

	return "", fmt.Errorf("script '%s' not found in allowed paths", scriptName)
}

func (e *PythonExecutor) prepareCommand(scriptPath string, args []string) (*exec.Cmd, error) {
	if err := os.Chmod(scriptPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to set executable permissions: %w", err)
	}

	// Use configured Python command to run the script
	cmdArgs := append([]string{scriptPath}, args...)
	cmd := exec.Command(e.pythonCommand, cmdArgs...)
	cmd.Dir = filepath.Dir(scriptPath)
	
	return cmd, nil
}

func (e *PythonExecutor) ExecuteWithTimeout(ctx context.Context, scriptName string, args []string, env []string, userID int) (*models.Task, error) {
	scriptPath, err := e.validateScriptPath(scriptName)
	if err != nil {
		return nil, err
	}

	taskID := uuid.New().String()
	task := &models.Task{
		ID:        taskID,
		Script:    scriptName,
		FullPath:  scriptPath,
		Args:      args,
		Env:       env,
		Status:    models.StatusPending,
		CreatedBy: userID,
		StartedAt: time.Now(),
	}

	if err := e.taskRepo.CreateTask(task); err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	// Run in background with timeout
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
		defer cancel()
		e.RunTask(ctx, taskID, scriptName, args, env, userID)
	}()

	return task, nil
}

func (e *PythonExecutor) RunTask(ctx context.Context, taskID, scriptName string, args []string, env []string, userID int) {
	scriptPath, err := e.validateScriptPath(scriptName)
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID).Msg("Script validation failed")
		return
	}

	cmd, err := e.prepareCommand(scriptPath, args)
	if err != nil {
		e.updateTaskFailure(taskID, err)
		return
	}

	// Add environment variables if provided
	if len(env) > 0 {
		cmd.Env = append(cmd.Env, env...)
	}

	// Store command
	e.runningTasks.Store(taskID, cmd)

	// Update status to running
	if err := e.updateTaskStatus(taskID, models.StatusRunning); err != nil {
		log.Error().Err(err).Str("task_id", taskID).Msg("Failed to update task status")
		return
	}

	// Execute and capture output
	output, err := cmd.CombinedOutput()
	e.runningTasks.Delete(taskID)

	// Handle context cancellation
	select {
	case <-ctx.Done():
		e.updateTaskCancelled(taskID, ctx.Err())
		return
	default:
	}

	// Update task status
	if err != nil {
		e.updateTaskFailure(taskID, err, output)
	} else {
		e.updateTaskSuccess(taskID, output)
	}
}

func (e *PythonExecutor) StopTask(taskID string) error {
	value, ok := e.runningTasks.Load(taskID)
	if !ok {
		return ErrTaskNotFound
	}

	cmd, ok := value.(*exec.Cmd)
	if !ok {
		return ErrInvalidTask
	}

	// Send SIGTERM first for graceful shutdown
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		log.Warn().Str("task_id", taskID).Err(err).Msg("Failed to send SIGTERM, trying SIGKILL")
		if err := cmd.Process.Kill(); err != nil {
			return err
		}
	}

	// Wait for process to exit
	_, err := cmd.Process.Wait()
	if err != nil {
		return err
	}

	// Update task status
	return e.updateTaskCancelled(taskID, errors.New("task stopped by user"))
}

// Helper methods for task status updates
func (e *PythonExecutor) updateTaskStatus(taskID string, status models.TaskStatus) error {
	task, err := e.taskRepo.GetTask(taskID)
	if err != nil {
		return err
	}

	task.Status = status
	return e.taskRepo.UpdateTask(task)
}

func (e *PythonExecutor) updateTaskSuccess(taskID string, output []byte) error {
	task, err := e.taskRepo.GetTask(taskID)
	if err != nil {
		return err
	}

	task.Status = models.StatusCompleted
	task.Output = string(output)
	task.ExitCode = 0
	task.EndedAt = time.Now()

	return e.taskRepo.UpdateTask(task)
}

func (e *PythonExecutor) updateTaskFailure(taskID string, execErr error, output ...[]byte) error {
	task, err := e.taskRepo.GetTask(taskID)
	if err != nil {
		return err
	}

	task.Status = models.StatusFailed
	task.Error = execErr.Error()
	task.EndedAt = time.Now()

	if len(output) > 0 {
		task.Output = string(output[0])
	}

	if exitErr, ok := execErr.(*exec.ExitError); ok {
		task.ExitCode = exitErr.ExitCode()
	} else {
		task.ExitCode = -1
	}

	return e.taskRepo.UpdateTask(task)
}

func (e *PythonExecutor) updateTaskCancelled(taskID string, reason error) error {
	task, err := e.taskRepo.GetTask(taskID)
	if err != nil {
		return err
	}

	task.Status = models.StatusCancelled
	task.Error = reason.Error()
	task.EndedAt = time.Now()
	task.ExitCode = -2

	return e.taskRepo.UpdateTask(task)
}

func (e *PythonExecutor) GetRunningTasks() []string {
	var tasks []string
	e.runningTasks.Range(func(key, value interface{}) bool {
		if taskID, ok := key.(string); ok {
			tasks = append(tasks, taskID)
		}
		return true
	})
	return tasks
}

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
	"github.com/rs/zerolog"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/Xecutables/Nebula.Conduit/internal/models"
	"github.com/Xecutables/Nebula.Conduit/internal/repository"
	"github.com/Xecutables/Nebula.Conduit/pkg/logger"
	"github.com/Xecutables/Nebula.Conduit/pkg/security"
)

var (
	ErrTaskNotFound = errors.New("task not found")
	ErrInvalidTask  = errors.New("invalid task")
)

type PythonExecutor struct {
	taskRepo      repository.TaskRepository
	runningTasks  sync.Map
	pathConfig    *config.PathConfig
	timeout       time.Duration
	validator     security.ScriptValidator
	pythonCommand string // Add this field
	logger        zerolog.Logger
}

func NewPythonExecutor(repo repository.TaskRepository, cfg *config.PathConfig, timeout time.Duration) *PythonExecutor {
	// Ensure the scripts directory exists
	if err := os.MkdirAll(cfg.PythonScriptsHome, 0755); err != nil {
		logger := logger.InitLogger()
		logger.Warn().Err(err).Msg("Failed to create Python scripts directory - Python execution may not work")
	}

	// Determine Python command based on platform or configuration
	pythonCmd := "python"
	if _, err := exec.LookPath("py"); err == nil {
		pythonCmd = "py"
	} else if _, err := exec.LookPath("python3"); err == nil {
		pythonCmd = "python3"
	}
	appLogger := logger.InitLogger()

	return &PythonExecutor{
		taskRepo:      repo,
		pathConfig:    cfg,
		timeout:       timeout,
		validator:     security.NewScriptValidator(),
		pythonCommand: pythonCmd,
		logger:        appLogger,
	}
}

func (e *PythonExecutor) validateScriptPath(scriptName string) (string, error) {
	// Add .py extension if not present
	if filepath.Ext(scriptName) == "" {
		scriptName = scriptName + ".py"
	}

	if err := e.validator.Validate(scriptName, nil); err != nil {
		e.logger.Error().Err(err).Str("script_name", scriptName).Msg("Script validation failed")
		return "", err
	}

	for _, basePath := range e.pathConfig.AllowedPaths {
		// Convert relative paths to absolute
		absBasePath := basePath
		if !filepath.IsAbs(basePath) {
			// If relative, resolve from current working directory
			cwd, err := os.Getwd()
			if err != nil {
				e.logger.Error().Err(err).Msg("Failed to get current working directory")
				continue
			}
			absBasePath = filepath.Join(cwd, basePath)
		}

		fullPath := filepath.Join(absBasePath, scriptName)
		e.logger.Debug().Str("checking_path", fullPath).Msg("Checking script path")

		if _, err := os.Stat(fullPath); err == nil {
			e.logger.Info().Str("found_path", fullPath).Msg("Script found")
			return fullPath, nil
		}
	}

	return "", fmt.Errorf("script %s not found in allowed paths: %v", scriptName, e.pathConfig.AllowedPaths)
}

func (e *PythonExecutor) prepareCommand(ctx context.Context, scriptPath string, args []string) (*exec.Cmd, error) {
	if err := os.Chmod(scriptPath, 0755); err != nil {
		e.logger.Error().Err(err).Str("script_path", scriptPath).Msg("Failed to set executable permissions for script")
		return nil, err
	}

	// Use configured Python command to run the script
	cmdArgs := append([]string{scriptPath}, args...)
	cmd := exec.CommandContext(ctx, e.pythonCommand, cmdArgs...)
	cmd.Dir = filepath.Dir(scriptPath)

	return cmd, nil
}

func (e *PythonExecutor) ExecuteWithTimeout(ctx context.Context, scriptName string, args []string, env []string, userID int) (*models.Task, error) {
	scriptPath, err := e.validateScriptPath(scriptName)
	if err != nil {
		e.logger.Error().Err(err).Str("script_name", scriptName).Msg("Script validation failed")
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
		e.logger.Error().Err(err).Msg("Failed to create task")
		return nil, err
	}

	// Run in background with timeout
	go func() {
		baseCtx := ctx
		if baseCtx == nil {
			baseCtx = context.Background()
		}

		ctx, cancel := context.WithTimeout(baseCtx, e.timeout)
		defer cancel()
		e.RunTask(ctx, taskID, scriptName, args, env, userID)
	}()

	return task, nil
}

func (e *PythonExecutor) RunTask(ctx context.Context, taskID, scriptName string, args []string, env []string, userID int) {
	scriptPath, err := e.validateScriptPath(scriptName)
	if err != nil {
		e.updateTaskFailure(taskID, err)
		e.logger.Error().Err(err).Str("task_id", taskID).Msg("Script validation failed")
		return
	}

	cmd, err := e.prepareCommand(ctx, scriptPath, args)
	e.logger.Info().Str("task_id", taskID).Str("script_path", scriptPath).Msg("Preparing command for task")
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
	defer e.runningTasks.Delete(taskID)

	// Update status to running
	if err := e.updateTaskStatus(taskID, models.StatusRunning); err != nil {
		e.logger.Error().Err(err).Str("task_id", taskID).Msg("Failed to update task status")
		return
	}

	// Execute and capture output
	e.logger.Info().Str("task_id", taskID).Msg("Executing task")
	output, err := cmd.CombinedOutput()

	if ctxErr := ctx.Err(); ctxErr != nil {
		e.updateTaskCancelled(taskID, ctxErr)
		e.logger.Warn().Str("task_id", taskID).Err(ctxErr).Msg("Task cancelled before terminal success/failure update")
		return
	}

	// Update task status
	if err != nil {
		e.updateTaskFailure(taskID, err, output)
		e.logger.Error().Str("task_id", taskID).Err(err).Msg(fmt.Sprintf("Task execution failed : %s", output))
		return
	}

	e.updateTaskSuccess(taskID, output)
	e.logger.Info().Str("task_id", taskID).Msg("Task completed successfully")
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
		e.logger.Warn().Str("task_id", taskID).Err(err).Msg("Failed to send SIGTERM, trying SIGKILL")
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
	}

	// Update task status
	return e.updateTaskCancelled(taskID, errors.New("task stopped by user"))
}

// Helper methods for task status updates
func (e *PythonExecutor) updateTaskStatus(taskID string, status models.TaskStatus) error {
	task, err := e.taskRepo.GetTask(taskID)
	if err != nil {
		e.logger.Error().Err(err).Str("task_id", taskID).Msg("Failed to get task for status update")
		return err
	}

	task.Status = status
	return e.taskRepo.UpdateTask(task)
}

func (e *PythonExecutor) updateTaskSuccess(taskID string, output []byte) error {
	return e.updateTaskTerminalState(taskID, func(task *models.Task) {
		task.Status = models.StatusCompleted
		task.Output = string(output)
		task.ExitCode = 0
		task.EndedAt = time.Now()
	})
}

func (e *PythonExecutor) updateTaskFailure(taskID string, execErr error, output ...[]byte) error {
	return e.updateTaskTerminalState(taskID, func(task *models.Task) {
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
	})
}

func (e *PythonExecutor) updateTaskCancelled(taskID string, reason error) error {
	return e.updateTaskTerminalState(taskID, func(task *models.Task) {
		task.Status = models.StatusCancelled
		task.Error = reason.Error()
		task.EndedAt = time.Now()
		task.ExitCode = -2
	})
}

func (e *PythonExecutor) updateTaskTerminalState(taskID string, update func(task *models.Task)) error {
	task, err := e.taskRepo.GetTask(taskID)
	if err != nil {
		e.logger.Error().Err(err).Str("task_id", taskID).Msg("Failed to get task for terminal status update")
		return err
	}

	if isTerminalStatus(task.Status) {
		return nil
	}

	update(task)
	return e.taskRepo.UpdateTask(task)
}

func isTerminalStatus(status models.TaskStatus) bool {
	switch status {
	case models.StatusCompleted, models.StatusFailed, models.StatusCancelled:
		return true
	default:
		return false
	}
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

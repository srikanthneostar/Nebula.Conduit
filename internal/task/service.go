package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Xecutables/Nebula.Conduit/internal/models"
	"github.com/Xecutables/Nebula.Conduit/pkg/executor"
	"github.com/Xecutables/Nebula.Conduit/pkg/security"
)

type Service interface {
	CreateTask(script string, args []string, env []string, userID int) (*models.Task, error)
	CreatePendingTask(script string, args []string, env []string, userID int) (*models.Task, error)
	RunTask(taskID string) error
	DeletePendingTask(taskID string) error
	GetTask(id string, userID int) (*models.Task, error)
	StopTask(id string, userID int) error
	ListTasks(userID int) ([]*models.Task, error)
}

type taskService struct {
	repo      Repository
	executor  *executor.PythonExecutor
	validator security.ScriptValidator
}

func NewTaskService(repo Repository, executor *executor.PythonExecutor, validator security.ScriptValidator) *taskService {
	return &taskService{
		repo:      repo,
		executor:  executor,
		validator: validator,
	}
}

func (s *taskService) CreateTask(script string, args []string, env []string, userID int) (*models.Task, error) {
	task, err := s.CreatePendingTask(script, args, env, userID)
	if err != nil {
		return nil, err
	}

	go func() {
		_ = s.RunTask(task.ID)
	}()

	return task, nil
}

func (s *taskService) CreatePendingTask(script string, args []string, env []string, userID int) (*models.Task, error) {
	// Validate script before proceeding
	if err := s.validator.Validate(script, args); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Generate Task ID
	taskID := uuid.New().String()

	task := &models.Task{
		ID:        taskID,
		Script:    script,
		Args:      args,
		Env:       env,
		Status:    models.StatusPending,
		CreatedBy: userID,
		StartedAt: time.Now(),
	}

	// Insert task into DB first (fully persistent before starting executor)
	if err := s.repo.CreateTask(task); err != nil {
		return nil, err
	}

	return task, nil
}

func (s *taskService) RunTask(taskID string) error {
	task, err := s.repo.GetTask(taskID)
	if err != nil {
		return err
	}

	if task.Status != models.StatusPending {
		return fmt.Errorf("only pending tasks can be started")
	}

	s.executor.RunTask(context.Background(), task.ID, task.Script, task.Args, task.Env, task.CreatedBy)
	return nil
}

func (s *taskService) DeletePendingTask(taskID string) error {
	task, err := s.repo.GetTask(taskID)
	if err != nil {
		return err
	}

	if task.Status != models.StatusPending {
		return fmt.Errorf("only pending tasks can be deleted")
	}

	return s.repo.DeleteTask(taskID)
}

func (s *taskService) GetTask(id string, userID int) (*models.Task, error) {
	task, err := s.repo.GetTask(id)
	if err != nil {
		return nil, err
	}

	if task.CreatedBy != userID {
		return nil, errors.New("unauthorized access to task")
	}

	return task, nil
}

func (s *taskService) StopTask(id string, userID int) error {
	task, err := s.repo.GetTask(id)
	if err != nil {
		return err
	}

	if task.CreatedBy != userID {
		return errors.New("unauthorized access to task")
	}

	if task.Status != models.StatusRunning {
		return errors.New("only running tasks can be stopped")
	}

	return s.executor.StopTask(id)
}

func (s *taskService) ListTasks(userID int) ([]*models.Task, error) {
	return s.repo.ListTasks(userID)
}

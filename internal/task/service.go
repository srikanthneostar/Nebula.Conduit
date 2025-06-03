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
	if err := s.validator.Validate(script, args); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

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

	// Run in background
	go s.executor.ExecuteWithTimeout(context.Background(), script, args, env, userID)

	return task, nil
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

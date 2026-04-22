package repository

import "github.com/Xecutables/Nebula.Conduit/internal/models"

// TaskRepository defines the interface for task storage operations
type TaskRepository interface {
	CreateTask(task *models.Task) error
	GetTask(id string) (*models.Task, error)
	UpdateTask(task *models.Task) error
	DeleteTask(id string) error
	ListTasks(userID int) ([]*models.Task, error)
}

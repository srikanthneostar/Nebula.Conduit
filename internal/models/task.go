package models

import "time"

// TaskStatus represents the possible states of a task
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusCancelled TaskStatus = "cancelled"
)

// Task represents an executable Python script task
type Task struct {
	ID        string     `json:"id"`
	Script    string     `json:"script"` // Script name (not full path)
	FullPath  string     `json:"-"`      // Internal full path
	Args      []string   `json:"args"`
	Env       []string   `json:"env"`
	Status    TaskStatus `json:"status"`
	Output    string     `json:"output"`
	Error     string     `json:"error"`
	ExitCode  int        `json:"exit_code"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   time.Time  `json:"ended_at"`
	CreatedBy int        `json:"created_by"`
}

// TaskRequest represents the data needed to create a new task
type TaskRequest struct {
	Script string   `json:"script" validate:"required"`
	Args   []string `json:"args"`
	Env    []string `json:"env"`
}

package task

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Xecutables/Nebula.Conduit/internal/models"
	"github.com/Xecutables/Nebula.Conduit/internal/repository"
)

var (
	ErrTaskNotFound = errors.New("task not found")
)

type Repository interface {
	CreateTask(task *models.Task) error
	GetTask(id string) (*models.Task, error)
	UpdateTask(task *models.Task) error
	DeleteTask(id string) error
	ListTasks(userID int) ([]*models.Task, error)
}

type SQLiteRepository struct {
	db *sql.DB
}

func NewSQLiteRepository(db *sql.DB) repository.TaskRepository {
	return &SQLiteRepository{db: db}
}

func (r *SQLiteRepository) CreateTask(task *models.Task) error {
	argsJSON, _ := json.Marshal(task.Args)
	envJSON, _ := json.Marshal(task.Env)

	_, err := r.db.Exec(
		`INSERT INTO tasks (
			id, script, full_path, args, env, status, 
			output, error, exit_code, started_at, ended_at, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.ID, task.Script, task.FullPath, argsJSON, envJSON, task.Status,
		task.Output, task.Error, task.ExitCode, task.StartedAt, task.EndedAt, task.CreatedBy,
	)
	return err
}

func (r *SQLiteRepository) GetTask(id string) (*models.Task, error) {
	var task models.Task
	var argsJSON, envJSON string

	err := r.db.QueryRow(
		`SELECT 
			id, script, full_path, args, env, status,
			output, error, exit_code, started_at, ended_at, created_by
		FROM tasks WHERE id = ?`,
		id,
	).Scan(
		&task.ID, &task.Script, &task.FullPath, &argsJSON, &envJSON, &task.Status,
		&task.Output, &task.Error, &task.ExitCode, &task.StartedAt, &task.EndedAt, &task.CreatedBy,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}

	json.Unmarshal([]byte(argsJSON), &task.Args)
	json.Unmarshal([]byte(envJSON), &task.Env)

	return &task, nil
}

func (r *SQLiteRepository) UpdateTask(task *models.Task) error {
	argsJSON, _ := json.Marshal(task.Args)
	envJSON, _ := json.Marshal(task.Env)

	result, err := r.db.Exec(
		`UPDATE tasks SET 
			script = ?, full_path = ?, args = ?, env = ?, status = ?,
			output = ?, error = ?, exit_code = ?, started_at = ?, ended_at = ?
		WHERE id = ?`,
		task.Script, task.FullPath, argsJSON, envJSON, task.Status,
		task.Output, task.Error, task.ExitCode, task.StartedAt, task.EndedAt, task.ID,
	)

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrTaskNotFound
	}

	return nil
}

func (r *SQLiteRepository) DeleteTask(id string) error {
	result, err := r.db.Exec(`DELETE FROM tasks WHERE id = ?`, id)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrTaskNotFound
	}

	return nil
}

func (r *SQLiteRepository) ListTasks(userID int) ([]*models.Task, error) {
	rows, err := r.db.Query(
		`SELECT 
			id, script, full_path, args, env, status,
			output, error, exit_code, started_at, ended_at, created_by
		FROM tasks WHERE created_by = ? ORDER BY started_at DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*models.Task
	for rows.Next() {
		var task models.Task
		var argsJSON, envJSON string

		err := rows.Scan(
			&task.ID, &task.Script, &task.FullPath, &argsJSON, &envJSON, &task.Status,
			&task.Output, &task.Error, &task.ExitCode, &task.StartedAt, &task.EndedAt, &task.CreatedBy,
		)
		if err != nil {
			return nil, err
		}

		json.Unmarshal([]byte(argsJSON), &task.Args)
		json.Unmarshal([]byte(envJSON), &task.Env)

		tasks = append(tasks, &task)
	}

	return tasks, nil
}

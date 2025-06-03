package auth

import (
	"database/sql"
	"errors"

	"github.com/Xecutables/Nebula.Conduit/internal/models"
)

type Repository interface {
	CreateUser(user *models.User) error
	GetUserByUsername(username string) (*models.User, error)
}

type SQLiteRepository struct {
	db *sql.DB
}

func NewSQLiteRepository(db *sql.DB) *SQLiteRepository {
	return &SQLiteRepository{db: db}
}

func (r *SQLiteRepository) CreateUser(user *models.User) error {
	_, err := r.db.Exec(
		"INSERT INTO users (username, password, email) VALUES (?, ?, ?)",
		user.Username, user.Password, user.Email,
	)
	return err
}

func (r *SQLiteRepository) GetUserByUsername(username string) (*models.User, error) {
	var user models.User
	err := r.db.QueryRow(
		"SELECT id, username, password, email FROM users WHERE username = ?",
		username,
	).Scan(&user.ID, &user.Username, &user.Password, &user.Email)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &user, nil
}

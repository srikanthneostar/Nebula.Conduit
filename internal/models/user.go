package models

import (
	"time"
)

// User represents a system user with authentication capabilities
type User struct {
	ID        int       `json:"id"`
	Username  string    `json:"username"`
	Password  string    `json:"-"` // Hidden from JSON serialization
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

// UserRegistration represents the data needed to register a new user
type UserRegistration struct {
	Username string `json:"username" validate:"required,min=3,max=50"`
	Password string `json:"password" validate:"required,min=8"`
	Email    string `json:"email" validate:"required,email"`
}

// UserLogin represents the data needed to authenticate a user
type UserLogin struct {
	Username string `json:"username" validate:"required"`
	Password string `json:"password" validate:"required"`
}

// PublicUser represents a user object safe for public consumption
type PublicUser struct {
	ID        int       `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

// ToPublic converts a User to its public representation
func (u *User) ToPublic() *PublicUser {
	return &PublicUser{
		ID:        u.ID,
		Username:  u.Username,
		Email:     u.Email,
		CreatedAt: u.CreatedAt,
	}
}

package auth

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/Xecutables/Nebula.Conduit/internal/models"
)

var (
	ErrUserExists         = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type Service interface {
	Register(username, password, email string) error
	Login(username, password string) (string, error)
	ValidateToken(token string) (int, error)
}

type AuthService struct {
	repo        Repository
	jwtSecret   string
	tokenExpiry time.Duration
}

func NewAuthService(repo Repository, jwtSecret string, tokenExpiryHours int) *AuthService {
	return &AuthService{
		repo:        repo,
		jwtSecret:   jwtSecret,
		tokenExpiry: time.Duration(tokenExpiryHours) * time.Hour,
	}
}

func (s *AuthService) Register(username, password, email string) error {
	existing, err := s.repo.GetUserByUsername(username)
	if err != nil {
		return err
	}
	if existing != nil {
		return ErrUserExists
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	user := &models.User{
		Username: username,
		Password: string(hashedPassword),
		Email:    email,
	}

	return s.repo.CreateUser(user)
}

func (s *AuthService) Login(username, password string) (string, error) {
	user, err := s.repo.GetUserByUsername(username)
	if err != nil {
		return "", err
	}
	if user == nil {
		return "", ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		return "", ErrInvalidCredentials
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": user.ID,
		"exp": time.Now().Add(s.tokenExpiry).Unix(),
	})

	return token.SignedString([]byte(s.jwtSecret))
}

func (s *AuthService) ValidateToken(tokenString string) (int, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(s.jwtSecret), nil
	})

	if err != nil {
		return 0, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		sub := claims["sub"].(float64)
		return int(sub), nil
	}

	return 0, errors.New("invalid token")
}

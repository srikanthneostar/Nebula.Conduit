package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"
	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/Xecutables/Nebula.Conduit/config"
	_ "github.com/Xecutables/Nebula.Conduit/docs" // Import generated docs
	"github.com/Xecutables/Nebula.Conduit/internal/auth"
	"github.com/Xecutables/Nebula.Conduit/internal/models"
	"github.com/Xecutables/Nebula.Conduit/internal/task"
	"github.com/Xecutables/Nebula.Conduit/pkg/executor"
	"github.com/Xecutables/Nebula.Conduit/pkg/security"
)

type Server struct {
	Router      *chi.Mux
	authService auth.Service
	taskService task.Service
	db          *sql.DB
	startTime   time.Time
}

type HealthResponse struct {
	Status    string            `json:"status" example:"ok"`
	Timestamp time.Time         `json:"timestamp" example:"2024-01-15T10:30:45Z"`
	Uptime    string            `json:"uptime" example:"2h15m30s"`
	Version   string            `json:"version,omitempty" example:"1.0.0"`
	Services  map[string]string `json:"services"`
}

type ErrorResponse struct {
	Error string `json:"error" example:"Invalid request"`
}

type TokenResponse struct {
	Token string `json:"token" example:"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."`
}

type MessageResponse struct {
	Message string `json:"message" example:"Operation completed successfully"`
}

// @title Nebula Conduit API
// @version 1.0
// @description A task execution service API
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name MIT
// @license.url https://opensource.org/licenses/MIT

// @host localhost:8080
// @BasePath /
// @schemes http https

// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Type "Bearer" followed by a space and JWT token.

func NewServer(db *sql.DB, cfg *config.Config) *Server {
	s := &Server{
		Router:    chi.NewRouter(),
		db:        db,
		startTime: time.Now(),
	}

	// Initialize services
	authRepo := auth.NewSQLiteRepository(db)
	s.authService = auth.NewAuthService(authRepo, cfg.Auth.JWTSecret, cfg.Auth.TokenExpiry)

	taskRepo := task.NewSQLiteRepository(db)
	validator := security.NewScriptValidator()
	executor := executor.NewPythonExecutor(taskRepo, cfg.GetPathConfig(), 1*time.Hour)
	s.taskService = task.NewTaskService(taskRepo, executor, validator)

	// Middleware
	s.Router.Use(middleware.RequestID)
	s.Router.Use(middleware.RealIP)
	s.Router.Use(middleware.Logger)
	s.Router.Use(middleware.Recoverer)
	s.Router.Use(middleware.Timeout(60 * time.Second))

	// Swagger documentation
	s.Router.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	// Health check endpoints (default and explicit)
	s.Router.Get("/", s.handleHealthCheck)
	s.Router.Get("/health", s.handleHealthCheck)

	// Public routes
	s.Router.Post("/login", s.handleLogin)
	s.Router.Post("/register", s.handleRegister)

	// Protected routes
	s.Router.Group(func(r chi.Router) {
		r.Use(s.authMiddleware)
		r.Post("/tasks", s.handleCreateTask)
		r.Get("/tasks/{id}", s.handleGetTask)
		r.Post("/tasks/{id}/stop", s.handleStopTask)
		r.Get("/tasks", s.handleListTasks)
	})

	return s
}

// handleHealthCheck godoc
// @Summary Health check endpoint
// @Description Get the health status of the service and its dependencies
// @Tags health
// @Accept json
// @Produce json
// @Success 200 {object} HealthResponse
// @Failure 503 {object} HealthResponse
// @Router / [get]
// @Router /health [get]
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(s.startTime)

	// Check database connectivity
	dbStatus := "ok"
	if err := s.db.Ping(); err != nil {
		log.Error().Err(err).Msg("Database health check failed")
		dbStatus = "error"
	}

	// Check services status
	services := map[string]string{
		"database": dbStatus,
		"auth":     "ok",
		"tasks":    "ok",
	}

	// Overall status
	status := "ok"
	for _, serviceStatus := range services {
		if serviceStatus != "ok" {
			status = "degraded"
			break
		}
	}

	response := HealthResponse{
		Status:    status,
		Timestamp: time.Now(),
		Uptime:    uptime.String(),
		Version:   "1.0.0",
		Services:  services,
	}

	// Set appropriate HTTP status code
	statusCode := http.StatusOK
	if status == "degraded" {
		statusCode = http.StatusServiceUnavailable
	}

	respondWithJSON(w, statusCode, response)
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenString := r.Header.Get("Authorization")
		if tokenString == "" {
			respondWithError(w, http.StatusUnauthorized, "Authorization header required")
			return
		}

		// Remove "Bearer " prefix if present
		if len(tokenString) > 7 && tokenString[:7] == "Bearer " {
			tokenString = tokenString[7:]
		}

		userID, err := s.authService.ValidateToken(tokenString)
		if err != nil {
			respondWithError(w, http.StatusUnauthorized, "Invalid token")
			return
		}

		ctx := context.WithValue(r.Context(), "userID", userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// handleLogin godoc
// @Summary User login
// @Description Authenticate user and return JWT token
// @Tags auth
// @Accept json
// @Produce json
// @Param request body models.UserLogin true "Login credentials"
// @Success 200 {object} TokenResponse
// @Failure 400 {object} ErrorResponse
// @Failure 401 {object} ErrorResponse
// @Router /login [post]
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req models.UserLogin
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	token, err := s.authService.Login(req.Username, req.Password)
	if err != nil {
		log.Warn().Str("username", req.Username).Msg("Failed login attempt")
		respondWithError(w, http.StatusUnauthorized, "Invalid credentials")
		return
	}

	log.Info().Str("username", req.Username).Msg("User logged in")
	respondWithJSON(w, http.StatusOK, TokenResponse{Token: token})
}

// handleRegister godoc
// @Summary User registration
// @Description Register a new user account
// @Tags auth
// @Accept json
// @Produce json
// @Param request body models.UserRegistration true "Registration details"
// @Success 201 {object} MessageResponse
// @Failure 400 {object} ErrorResponse
// @Router /register [post]
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req models.UserRegistration
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if err := s.authService.Register(req.Username, req.Password, req.Email); err != nil {
		respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Info().Str("username", req.Username).Msg("New user registered")
	respondWithJSON(w, http.StatusCreated, MessageResponse{Message: "User created successfully"})
}

// handleCreateTask godoc
// @Summary Create a new task
// @Description Create and execute a new task with the provided script
// @Tags tasks
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param request body models.TaskRequest true "Task details"
// @Success 201 {object} models.Task
// @Failure 400 {object} ErrorResponse
// @Failure 401 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /tasks [post]
func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req models.TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	userID := r.Context().Value("userID").(int)
	task, err := s.taskService.CreateTask(req.Script, req.Args, req.Env, userID)
	if err != nil {
		log.Error().Err(err).Str("script", req.Script).Msg("Failed to create task")
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Info().Str("task_id", task.ID).Str("script", req.Script).Msg("Task created")
	respondWithJSON(w, http.StatusCreated, task)
}

// handleGetTask godoc
// @Summary Get task by ID
// @Description Retrieve a specific task by its ID
// @Tags tasks
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param id path string true "Task ID"
// @Success 200 {object} models.Task
// @Failure 401 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /tasks/{id} [get]
func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	userID := r.Context().Value("userID").(int)

	task, err := s.taskService.GetTask(taskID, userID)
	if err != nil {
		log.Error().Err(err).Str("task_id", taskID).Msg("Failed to get task")
		respondWithError(w, http.StatusNotFound, "Task not found")
		return
	}

	respondWithJSON(w, http.StatusOK, task)
}

// handleStopTask godoc
// @Summary Stop a running task
// @Description Stop the execution of a running task
// @Tags tasks
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param id path string true "Task ID"
// @Success 200 {object} MessageResponse
// @Failure 401 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /tasks/{id}/stop [post]
func (s *Server) handleStopTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	userID := r.Context().Value("userID").(int)

	if err := s.taskService.StopTask(taskID, userID); err != nil {
		log.Error().Err(err).Str("task_id", taskID).Msg("Failed to stop task")
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Info().Str("task_id", taskID).Msg("Task stopped")
	respondWithJSON(w, http.StatusOK, MessageResponse{Message: "Task stopped successfully"})
}

// handleListTasks godoc
// @Summary List user tasks
// @Description Get a list of all tasks for the authenticated user
// @Tags tasks
// @Accept json
// @Produce json
// @Security BearerAuth
// @Success 200 {array} models.Task
// @Failure 401 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /tasks [get]
func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value("userID").(int)

	tasks, err := s.taskService.ListTasks(userID)
	if err != nil {
		log.Error().Err(err).Int("user_id", userID).Msg("Failed to list tasks")
		respondWithError(w, http.StatusInternalServerError, "Failed to retrieve tasks")
		return
	}

	if tasks == nil {
		tasks = []*models.Task{} // Return empty array instead of null
	}

	respondWithJSON(w, http.StatusOK, tasks)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, ErrorResponse{Error: message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, err := json.Marshal(payload)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal response")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

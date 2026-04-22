package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"
	httpSwagger "github.com/swaggo/http-swagger"
	"golang.org/x/time/rate"

	"github.com/Xecutables/Nebula.Conduit/config"
	_ "github.com/Xecutables/Nebula.Conduit/docs" // Import generated docs
	"github.com/Xecutables/Nebula.Conduit/internal/auth"
	"github.com/Xecutables/Nebula.Conduit/internal/models"
	"github.com/Xecutables/Nebula.Conduit/internal/task"
	"github.com/Xecutables/Nebula.Conduit/pkg/database"
	"github.com/Xecutables/Nebula.Conduit/pkg/executor"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
	"github.com/Xecutables/Nebula.Conduit/pkg/pipeline/components"
	"github.com/Xecutables/Nebula.Conduit/pkg/security"
)

// Backpressure configuration
type BackpressureConfig struct {
	MaxConcurrentTasks    int           `json:"max_concurrent_tasks"`
	MaxQueueSize          int           `json:"max_queue_size"`
	RateLimitPerSecond    float64       `json:"rate_limit_per_second"`
	RateLimitBurst        int           `json:"rate_limit_burst"`
	CircuitBreakerTimeout time.Duration `json:"circuit_breaker_timeout"`
	MaxMemoryUsagePercent float64       `json:"max_memory_usage_percent"`
	MaxCPUUsagePercent    float64       `json:"max_cpu_usage_percent"`
}

// Circuit breaker states
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// Circuit breaker for preventing cascade failures
type CircuitBreaker struct {
	mu           sync.RWMutex
	state        CircuitState
	failures     int64
	lastFailTime time.Time
	timeout      time.Duration
	maxFailures  int64
}

// Rate limiter per user
type UserRateLimiter struct {
	limiters sync.Map // map[int]*rate.Limiter for user ID -> limiter
	rate     rate.Limit
	burst    int
}

// Task queue for managing concurrent execution
type TaskQueue struct {
	mu           sync.RWMutex
	queue        chan *queuedTask
	running      int64
	maxRunning   int64
	maxQueueSize int
}

type queuedTask struct {
	TaskID string
	UserID int
}

// Resource monitor for system health
type ResourceMonitor struct {
	mu               sync.RWMutex
	maxMemoryPercent float64
	maxCPUPercent    float64
	lastMemoryCheck  time.Time
	lastCPUCheck     time.Time
	memoryUsage      float64
	cpuUsage         float64
	checkInterval    time.Duration
	now              func() time.Time
	readProcessRSS   func() (uint64, error)
	readMemoryLimit  func() (uint64, error)
	readProcessCPU   func() (float64, error)
	readCPUCapacity  func() (float64, error)
}

type Server struct {
	Router      *chi.Mux
	authService auth.Service
	taskService task.Service
	db          *sql.DB
	startTime   time.Time

	// Pipeline engine
	pipelineEngine *pipeline.PipelineEngine

	// Backpressure components
	backpressureConfig *BackpressureConfig
	circuitBreaker     *CircuitBreaker
	rateLimiter        *UserRateLimiter
	taskQueue          *TaskQueue
	resourceMonitor    *ResourceMonitor
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
	// Default backpressure configuration
	backpressureConfig := &BackpressureConfig{
		MaxConcurrentTasks:    10,
		MaxQueueSize:          100,
		RateLimitPerSecond:    10.0,
		RateLimitBurst:        20,
		CircuitBreakerTimeout: 30 * time.Second,
		MaxMemoryUsagePercent: 80.0,
		MaxCPUUsagePercent:    80.0,
	}

	s := &Server{
		Router:             chi.NewRouter(),
		db:                 db,
		startTime:          time.Now(),
		backpressureConfig: backpressureConfig,
		circuitBreaker: &CircuitBreaker{
			timeout:     backpressureConfig.CircuitBreakerTimeout,
			maxFailures: 5,
		},
		rateLimiter: &UserRateLimiter{
			rate:  rate.Limit(backpressureConfig.RateLimitPerSecond),
			burst: backpressureConfig.RateLimitBurst,
		},
		taskQueue: &TaskQueue{
			queue:        make(chan *queuedTask, backpressureConfig.MaxQueueSize),
			maxRunning:   int64(backpressureConfig.MaxConcurrentTasks),
			maxQueueSize: backpressureConfig.MaxQueueSize,
		},
		resourceMonitor: newResourceMonitor(backpressureConfig.MaxMemoryUsagePercent, backpressureConfig.MaxCPUUsagePercent, 5*time.Second),
	}

	// Initialize services
	authRepo := auth.NewSQLiteRepository(db)
	s.authService = auth.NewAuthService(authRepo, cfg.Auth.JWTSecret, cfg.Auth.TokenExpiry)

	taskRepo := task.NewSQLiteRepository(db)
	validator := security.NewScriptValidator()
	executor := executor.NewPythonExecutor(taskRepo, cfg.GetPathConfig(), 1*time.Hour)
	s.taskService = task.NewTaskService(taskRepo, executor, validator)

	// Initialize MongoDB queue for backpressure (optional)
	var mongoQueue *pipeline.MongoDBQueue
	if cfg.MongoDB.Enabled && cfg.MongoDB.ConnectionString != "" {
		log.Info().Str("connection_string", cfg.MongoDB.ConnectionString).Msg("Initializing MongoDB queue for backpressure")
		var err error
		mongoQueue, err = database.InitMongoDB(
			cfg.MongoDB.ConnectionString,
			cfg.MongoDB.DatabaseName,
			cfg.MongoDB.Username,
			cfg.MongoDB.Password,
		)
		if err != nil {
			log.Error().Err(err).Msg("Failed to initialize MongoDB queue")
		} else {
			log.Info().Msg("MongoDB queue initialized successfully")
		}
	}

	// Initialize pipeline engine
	pipelineFactory := pipeline.NewComponentFactory()
	components.RegisterComponents(pipelineFactory)
	pipelineRepo := pipeline.NewSQLPipelineRepository(db)
	pipelineMetrics := pipeline.NewDefaultMetricsCollector()
	pipelineConfig := pipeline.PipelineEngineConfig{
		MaxConcurrentInstances:    10,
		MaxGoroutinesPerInstance:  50,
		ExecutionHistoryRetention: 30,
	}

	// Create backpressure system with MongoDB queue
	var backpressure pipeline.BackpressureSystem
	if mongoQueue != nil {
		backpressure = pipeline.NewQueueBackpressureSystem(mongoQueue)
	}

	s.pipelineEngine = pipeline.NewPipelineEngine(db, pipelineRepo, pipelineFactory, backpressure, &log.Logger, pipelineMetrics, pipelineConfig)

	// Configure GraphQL sync for remote pipeline loading at startup
	gqlCfg := pipeline.ResolveGraphQLConfig(cfg.GraphQL.Endpoint, cfg.GraphQL.Username, cfg.GraphQL.Password)
	s.pipelineEngine.SetGraphQLConfig(gqlCfg)

	// Initialize the pipeline engine to start scheduler and load active pipelines
	if err := s.pipelineEngine.Initialize(context.Background()); err != nil {
		log.Error().Err(err).Msg("Failed to initialize pipeline engine")
	} else {
		log.Info().Msg("Pipeline engine initialized successfully")
	}

	// Start background workers
	s.startTaskQueueWorker()
	s.startResourceMonitor()

	// Middleware
	s.Router.Use(middleware.RequestID)
	s.Router.Use(middleware.RealIP)
	s.Router.Use(middleware.Logger)
	s.Router.Use(middleware.Recoverer)
	s.Router.Use(middleware.Timeout(60 * time.Second))

	// Backpressure middleware
	s.Router.Use(s.backpressureMiddleware)

	// Swagger documentation
	s.Router.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	// Health check endpoints (default and explicit)
	s.Router.Get("/", s.handleHealthCheck)
	s.Router.Get("/health", s.handleHealthCheck)
	s.Router.Get("/metrics", s.handleMetrics)

	// Public routes
	s.Router.Post("/login", s.handleLogin)
	s.Router.Post("/register", s.handleRegister)

	// Protected routes
	s.Router.Group(func(r chi.Router) {
		r.Use(s.authMiddleware)
		r.Use(s.rateLimitMiddleware)
		r.Post("/tasks", s.handleCreateTask)
		r.Get("/tasks/{id}", s.handleGetTask)
		r.Post("/tasks/{id}/stop", s.handleStopTask)
		r.Get("/tasks", s.handleListTasks)

		// Pipeline engine routes - use the server's pipeline engine instance
		pipeline.RegisterRoutes(r, s.pipelineEngine)
	})

	return s
}

// Backpressure middleware - monitors system health and rejects requests when overloaded
func (s *Server) backpressureMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check circuit breaker
		if !s.circuitBreaker.Allow() {
			log.Warn().Msg("Circuit breaker is open, rejecting request")
			respondWithError(w, http.StatusServiceUnavailable, "Service temporarily unavailable")
			return
		}

		// Check resource usage
		if s.resourceMonitor.IsOverloaded() {
			log.Warn().Msg("System overloaded, rejecting request")
			respondWithError(w, http.StatusServiceUnavailable, "System overloaded")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Rate limiting middleware per user
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, ok := auth.UserIDFromContext(r.Context())
		if !ok {
			respondWithError(w, http.StatusUnauthorized, "Authentication context missing")
			return
		}

		limiter := s.rateLimiter.GetLimiter(userID)
		if !limiter.Allow() {
			log.Warn().Int("user_id", userID).Msg("Rate limit exceeded")
			respondWithError(w, http.StatusTooManyRequests, "Rate limit exceeded")
			return
		}

		next.ServeHTTP(w, r)
	})
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

		next.ServeHTTP(w, r.WithContext(auth.WithUserID(r.Context(), userID)))
	})
}

// Circuit Breaker methods
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailTime) > cb.timeout {
			cb.mu.RUnlock()
			cb.mu.Lock()
			cb.state = CircuitHalfOpen
			cb.mu.Unlock()
			cb.mu.RLock()
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = CircuitClosed
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailTime = time.Now()
	if cb.failures >= cb.maxFailures {
		cb.state = CircuitOpen
	}
}

// Rate Limiter methods
func (rl *UserRateLimiter) GetLimiter(userID int) *rate.Limiter {
	if limiter, exists := rl.limiters.Load(userID); exists {
		return limiter.(*rate.Limiter)
	}

	limiter := rate.NewLimiter(rl.rate, rl.burst)
	rl.limiters.Store(userID, limiter)
	return limiter
}

// Task Queue methods
func (tq *TaskQueue) Enqueue(task *queuedTask) error {
	tq.mu.RLock()
	queueLen := len(tq.queue)
	tq.mu.RUnlock()

	if queueLen >= tq.maxQueueSize {
		return fmt.Errorf("task queue is full (size: %d)", queueLen)
	}

	select {
	case tq.queue <- task:
		return nil
	default:
		return fmt.Errorf("task queue is full")
	}
}

func (tq *TaskQueue) CanAcceptTask() bool {
	running := atomic.LoadInt64(&tq.running)
	return running < tq.maxRunning
}

func (tq *TaskQueue) TryStartTask() bool {
	for {
		running := atomic.LoadInt64(&tq.running)
		if running >= tq.maxRunning {
			return false
		}

		if atomic.CompareAndSwapInt64(&tq.running, running, running+1) {
			return true
		}
	}
}

func (tq *TaskQueue) IncrementRunning() {
	atomic.AddInt64(&tq.running, 1)
}

func (tq *TaskQueue) DecrementRunning() {
	atomic.AddInt64(&tq.running, -1)
}

func (tq *TaskQueue) GetStats() (int, int64) {
	tq.mu.RLock()
	queueSize := len(tq.queue)
	tq.mu.RUnlock()
	running := atomic.LoadInt64(&tq.running)
	return queueSize, running
}

// Resource Monitor methods
func (rm *ResourceMonitor) IsOverloaded() bool {
	rm.ensureFreshStats()

	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.memoryUsage > rm.maxMemoryPercent || rm.cpuUsage > rm.maxCPUPercent
}

func (rm *ResourceMonitor) GetStats() (float64, float64) {
	rm.ensureFreshStats()

	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.memoryUsage, rm.cpuUsage
}

// Background workers
func (s *Server) startTaskQueueWorker() {
	go func() {
		for task := range s.taskQueue.queue {
			for !s.taskQueue.TryStartTask() {
				time.Sleep(100 * time.Millisecond)
			}

			s.launchTask(task)
		}
	}()
}

func (s *Server) launchTask(task *queuedTask) {
	go func(item *queuedTask) {
		defer s.taskQueue.DecrementRunning()

		if err := s.taskService.RunTask(item.TaskID); err != nil {
			s.circuitBreaker.RecordFailure()
			log.Error().
				Err(err).
				Str("task_id", item.TaskID).
				Int("user_id", item.UserID).
				Msg("Task execution failed")
			return
		}

		s.circuitBreaker.RecordSuccess()
	}(task)
}

func (s *Server) startResourceMonitor() {
	go func() {
		ticker := time.NewTicker(s.resourceMonitor.checkInterval)
		defer ticker.Stop()

		for range ticker.C {
			s.resourceMonitor.updateResourceUsage()

			memUsage, cpuUsage := s.resourceMonitor.GetStats()
			if memUsage > s.resourceMonitor.maxMemoryPercent {
				log.Warn().Float64("memory_usage", memUsage).Msg("High memory usage detected")
			}
			if cpuUsage > s.resourceMonitor.maxCPUPercent {
				log.Warn().Float64("cpu_usage", cpuUsage).Msg("High CPU usage detected")
			}
		}
	}()
}

// handleMetrics godoc
// @Summary System metrics endpoint
// @Description Get system metrics including backpressure statistics
// @Tags metrics
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /metrics [get]
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	queueSize, runningTasks := s.taskQueue.GetStats()
	memUsage, cpuUsage := s.resourceMonitor.GetStats()

	metrics := map[string]interface{}{
		"timestamp": time.Now(),
		"uptime":    time.Since(s.startTime).String(),
		"backpressure": map[string]interface{}{
			"circuit_breaker_state":    s.circuitBreaker.state,
			"circuit_breaker_failures": atomic.LoadInt64(&s.circuitBreaker.failures),
			"task_queue_size":          queueSize,
			"running_tasks":            runningTasks,
			"max_concurrent_tasks":     s.backpressureConfig.MaxConcurrentTasks,
			"max_queue_size":           s.backpressureConfig.MaxQueueSize,
		},
		"resources": map[string]interface{}{
			"memory_usage_percent": memUsage,
			"cpu_usage_percent":    cpuUsage,
			"max_memory_percent":   s.resourceMonitor.maxMemoryPercent,
			"max_cpu_percent":      s.resourceMonitor.maxCPUPercent,
		},
		"rate_limiting": map[string]interface{}{
			"rate_per_second": float64(s.rateLimiter.rate),
			"burst_size":      s.rateLimiter.burst,
		},
	}

	respondWithJSON(w, http.StatusOK, metrics)
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

// handleCreateTask godoc - Enhanced with backpressure
// @Summary Create a new task
// @Description Create and execute a new task with the provided script using backpressure controls
// @Tags tasks
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param request body models.TaskRequest true "Task details"
// @Success 201 {object} models.Task
// @Success 202 {object} models.Task "Task queued due to backpressure"
// @Failure 400 {object} ErrorResponse
// @Failure 401 {object} ErrorResponse
// @Failure 429 {object} ErrorResponse "Rate limit exceeded"
// @Failure 500 {object} ErrorResponse
// @Failure 503 {object} ErrorResponse "Service overloaded"
// @Router /tasks [post]
func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req models.TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondWithError(w, http.StatusUnauthorized, "Authentication context missing")
		return
	}

	// Check if we can process immediately or need to queue
	if s.taskQueue.TryStartTask() {
		// Process immediately
		task, err := s.taskService.CreatePendingTask(req.Script, req.Args, req.Env, userID)
		if err != nil {
			s.taskQueue.DecrementRunning()
			s.circuitBreaker.RecordFailure()
			log.Error().Err(err).Str("script", req.Script).Msg("Failed to create task")
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		s.launchTask(&queuedTask{TaskID: task.ID, UserID: userID})
		log.Info().Str("task_id", task.ID).Str("script", req.Script).Msg("Task created immediately")
		respondWithJSON(w, http.StatusCreated, task)
		return
	}

	task, err := s.taskService.CreatePendingTask(req.Script, req.Args, req.Env, userID)
	if err != nil {
		s.circuitBreaker.RecordFailure()
		log.Error().Err(err).Str("script", req.Script).Msg("Failed to create queued task")
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := s.taskQueue.Enqueue(&queuedTask{TaskID: task.ID, UserID: userID}); err != nil {
		if deleteErr := s.taskService.DeletePendingTask(task.ID); deleteErr != nil {
			log.Error().
				Err(deleteErr).
				Str("task_id", task.ID).
				Msg("Failed to delete pending task after queue rejection")
		}

		log.Warn().
			Err(err).
			Str("script", req.Script).
			Str("task_id", task.ID).
			Msg("Failed to queue task")
		respondWithError(w, http.StatusServiceUnavailable, "Task queue is full, please try again later")
		return
	}

	log.Info().Str("task_id", task.ID).Str("script", req.Script).Msg("Task queued due to backpressure")
	respondWithJSON(w, http.StatusAccepted, task)
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
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondWithError(w, http.StatusUnauthorized, "Authentication context missing")
		return
	}

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
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondWithError(w, http.StatusUnauthorized, "Authentication context missing")
		return
	}

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
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok {
		respondWithError(w, http.StatusUnauthorized, "Authentication context missing")
		return
	}

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

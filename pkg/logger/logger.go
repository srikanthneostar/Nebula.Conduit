package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Xecutables/Nebula.Conduit/config"
	"github.com/natefinch/lumberjack"
	"github.com/rs/zerolog"
)

func InitLogger() zerolog.Logger {
	envValue := os.Getenv("NEBULA_CONDUIT_HOME")
	if envValue == "" {
		fmt.Println("Environment variable NEBULA_CONDUIT_HOME is not set.")
		envValue = "." // fallback to current directory
	}

	configPath := filepath.Join(envValue, "config.yaml")

	// Use absolute path for log file
	logPath := filepath.Join(envValue, "logs", "app.log")

	// Ensure logs directory exists
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		fmt.Printf("Failed to create log directory: %v\n", err)
	}

	rotator := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    50,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
	}

	logLevelStr := cfg.Log.Level
	logLevel, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		fmt.Printf("Error parsing log level: %v\n", err)
		logLevel = zerolog.InfoLevel // fallback to InfoLevel
	}

	multi := zerolog.MultiLevelWriter(
		zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339},
		rotator,
	)

	logger := zerolog.New(multi).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(logLevel)

	fmt.Printf("Logger initialized. Writing to: %s\n", logPath)

	return logger
}

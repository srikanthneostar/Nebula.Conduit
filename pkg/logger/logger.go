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

func InitLogger(name ...string) zerolog.Logger {
	envValue := os.Getenv("NEBULA_CONDUIT_HOME")
	if envValue == "" {
		fmt.Println("Environment variable NEBULA_CONDUIT_HOME is not set.")
		envValue = "." // fallback to current directory
	}

	configPath := filepath.Join(envValue, "config.yaml")

	// Use logger name for the log filename if provided, otherwise default to "app"
	logName := "app"
	if len(name) > 0 && name[0] != "" {
		logName = name[0]
	}
	logPath := filepath.Join(envValue, "logs", logName+".log")

	// Ensure logs directory exists
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		fmt.Printf("Failed to create log directory: %v\n", err)
	}

	// Rolling file: rotates at 50MB, keeps 7 backups up to 30 days, compresses old files
	rotator := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    50,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
		LocalTime:  true,
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

	logContext := zerolog.New(multi).With().Timestamp()
	if len(name) > 0 && name[0] != "" {
		logContext = logContext.Str("logger", name[0])
	}
	logger := logContext.Logger()
	zerolog.SetGlobalLevel(logLevel)

	fmt.Printf("Logger initialized. Writing to: %s\n", logPath)

	return logger
}

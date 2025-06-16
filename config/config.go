package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Server struct {
		Host string `mapstructure:"HOST"`
		Port int    `mapstructure:"PORT"`
	} `mapstructure:"server"`

	Auth struct {
		JWTSecret   string `mapstructure:"JWT_SECRET"`
		TokenExpiry int    `mapstructure:"TOKEN_EXPIRY_HOURS"`
	} `mapstructure:"auth"`

	Database struct {
		Path string `mapstructure:"PATH"`
	} `mapstructure:"database"`

	Paths struct {
		PythonScriptsHome string   `mapstructure:"PYTHON_SCRIPTS_HOME"`
		AllowedPaths      []string `mapstructure:"ALLOWED_PATHS"`
	} `mapstructure:"paths"`

	Log struct {
		Level string `mapstructure:"LOGLEVEL"`
	} `mapstructure:"setlog"`
}

func LoadConfig(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

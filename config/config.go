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

	MongoDB struct {
		Enabled          bool   `mapstructure:"enabled"`
		ConnectionString string `mapstructure:"connection_string"`
		DatabaseName     string `mapstructure:"database_name"`
		Username         string `mapstructure:"username"`
		Password         string `mapstructure:"password"`
	} `mapstructure:"mongodb"`

	TLSConfig struct {
		Enabled  bool   `mapstructure:"enabled"`
		CertFile string `mapstructure:"cert_file"`
		KeyFile  string `mapstructure:"key_file"`
	} `mapstructure:"tls_config"`

	Paths struct {
		PythonScriptsHome string   `mapstructure:"PYTHON_SCRIPTS_HOME"`
		AllowedPaths      []string `mapstructure:"ALLOWED_PATHS"`
	} `mapstructure:"paths"`

	Log struct {
		Level string `mapstructure:"LOGLEVEL"`
	} `mapstructure:"setlog"`

	S3 struct {
		Endpoint  string `mapstructure:"S3_ENDPOINT"`
		Bucket    string `mapstructure:"S3_BUCKET"`
		UseSSL    string `mapstructure:"S3_USE_SSL"`
		Region    string `mapstructure:"S3_REGION"`
		AccessKey string `mapstructure:"S3_ACCESS_KEY"`
		SecretKey string `mapstructure:"S3_SECRET_KEY"`
	} `mapstructure:"s3"`

	GraphQL struct {
		Endpoint string `mapstructure:"GRAPHQL_ENDPOINT"`
		Username string `mapstructure:"GRAPHQL_USERNAME"`
		Password string `mapstructure:"GRAPHQL_PASSWORD"`
	} `mapstructure:"graphql"`
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

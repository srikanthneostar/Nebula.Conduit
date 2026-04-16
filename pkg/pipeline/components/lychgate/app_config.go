package lychgate

import (
	"fmt"
	"time"
)

// AppConfig represents a row in the app_configs table.
type AppConfig struct {
	ID                  int       `json:"id"`
	Key                 string    `json:"key"`
	Value               string    `json:"value"`
	Category            string    `json:"category"`
	CategoryDescription string    `json:"category_description"`
	Description         string    `json:"description"`
	IsEncrypted         bool      `json:"is_encrypted"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// GetByKey returns the app_configs row matching the given key, or an error if not found.
func (l *LychgateResponseComponent) GetByKey(key string) (*AppConfig, error) {
	row := l.db.QueryRow(
		`SELECT id, key, value, category, category_description, description,
		        is_encrypted, created_at, updated_at
		   FROM app_configs WHERE key = ?`, key,
	)

	var cfg AppConfig
	var isEnc int
	if err := row.Scan(
		&cfg.ID, &cfg.Key, &cfg.Value, &cfg.Category,
		&cfg.CategoryDescription, &cfg.Description,
		&isEnc, &cfg.CreatedAt, &cfg.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("app_configs: key %q not found: %w", key, err)
	}
	cfg.IsEncrypted = isEnc == 1
	return &cfg, nil
}

// GetByCategory returns all app_configs rows that belong to the given category.
func (l *LychgateResponseComponent) GetByCategory(category string) ([]AppConfig, error) {
	rows, err := l.db.Query(
		`SELECT id, key, value, category, category_description, description,
		        is_encrypted, created_at, updated_at
		   FROM app_configs WHERE category = ?`, category,
	)
	if err != nil {
		return nil, fmt.Errorf("app_configs: failed to query category %q: %w", category, err)
	}
	defer rows.Close()

	var configs []AppConfig
	for rows.Next() {
		var cfg AppConfig
		var isEnc int
		if err := rows.Scan(
			&cfg.ID, &cfg.Key, &cfg.Value, &cfg.Category,
			&cfg.CategoryDescription, &cfg.Description,
			&isEnc, &cfg.CreatedAt, &cfg.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("app_configs: failed to scan row: %w", err)
		}
		cfg.IsEncrypted = isEnc == 1
		configs = append(configs, cfg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("app_configs: row iteration error: %w", err)
	}
	return configs, nil
}

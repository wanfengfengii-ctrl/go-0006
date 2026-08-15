// Package config holds QuotaRaft's runtime configuration.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config is the runtime configuration loaded from a JSON file or environment.
type Config struct {
	HTTPAddr      string        `json:"http_addr"`
	DBPath        string        `json:"db_path"`
	TestMode      bool          `json:"test_mode"`
	Maintenance   MaintenanceConfig `json:"maintenance"`
	LogLevel      string        `json:"log_level"`
	// TestClockStart is the initial logical time for the manual clock in test
	// mode, as a Unix timestamp in seconds.
	TestClockStart int64 `json:"test_clock_start"`
}

// MaintenanceConfig mirrors the service maintenance configuration.
type MaintenanceConfig struct {
	Enabled     bool          `json:"enabled"`
	MinInterval time.Duration `json:"min_interval"`
}

// Default returns a production configuration with sensible defaults.
func Default() Config {
	return Config{
		HTTPAddr: ":8080",
		DBPath:   "/var/lib/quotaraft/quotaraft.db",
		Maintenance: MaintenanceConfig{
			Enabled:     true,
			MinInterval: 500 * time.Millisecond,
		},
		LogLevel: "info",
	}
}

// Load reads a JSON config file. Missing fields keep their zero/default value.
func Load(path string) (Config, error) {
	c := Default()
	if path == "" {
		// Allow env overrides when no file is given.
		if v := os.Getenv("QUOTARAFT_HTTP_ADDR"); v != "" {
			c.HTTPAddr = v
		}
		if v := os.Getenv("QUOTARAFT_DB_PATH"); v != "" {
			c.DBPath = v
		}
		return c, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("parse config %s: %w", path, err)
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8080"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	return c, nil
}

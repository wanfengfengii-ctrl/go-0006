package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"http_addr":":9090","db_path":"/tmp/q.db","test_mode":true,"log_level":"debug"}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9090" || c.DBPath != "/tmp/q.db" || !c.TestMode || c.LogLevel != "debug" {
		t.Fatalf("unexpected config: %+v", c)
	}
}

func TestLoadDefault(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" {
		t.Fatalf("default addr = %s", c.HTTPAddr)
	}
}

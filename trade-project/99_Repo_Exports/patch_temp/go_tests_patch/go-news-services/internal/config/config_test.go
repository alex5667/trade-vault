package config

import (
	"os"
	"testing"
	"time"
)

func TestFromEnvDefaults(t *testing.T) {
	// Ensure envs are empty
	_ = os.Unsetenv("POLL_INTERVAL")
	_ = os.Unsetenv("NEWS_UID_BUCKET")
	cfg := FromEnv()
	if cfg.PollInterval != 15*time.Second {
		t.Fatalf("default PollInterval: got %v", cfg.PollInterval)
	}
	if cfg.NewsUIDBucket != 6*time.Hour {
		t.Fatalf("default NewsUIDBucket: got %v", cfg.NewsUIDBucket)
	}
	if cfg.DedupeTTL != 7*24*time.Hour {
		t.Fatalf("default DedupeTTL: got %v", cfg.DedupeTTL)
	}
}

func TestFromEnvParsesDurations(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "5s")
	t.Setenv("NEWS_UID_BUCKET", "2h")
	t.Setenv("NEWS_INGESTOR_HEARTBEAT_TTL", "10s")
	cfg := FromEnv()
	if cfg.PollInterval != 5*time.Second {
		t.Fatalf("PollInterval: got %v", cfg.PollInterval)
	}
	if cfg.NewsUIDBucket != 2*time.Hour {
		t.Fatalf("NewsUIDBucket: got %v", cfg.NewsUIDBucket)
	}
	if cfg.HeartbeatTTL != 10*time.Second {
		t.Fatalf("HeartbeatTTL: got %v", cfg.HeartbeatTTL)
	}
}

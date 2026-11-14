package sender

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

// MockConfig holds mock sender tuning from the environment.
type MockConfig struct {
	SendDelay   time.Duration
	FailureRate float64
}

// FloatSource returns a value in [0, 1) for failure simulation.
type FloatSource interface {
	Float64() float64
}

// LoadMockConfig reads MOCK_SEND_DELAY and MOCK_FAILURE_RATE.
func LoadMockConfig() (MockConfig, error) {
	cfg := MockConfig{}
	if v := os.Getenv("MOCK_SEND_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return MockConfig{}, fmt.Errorf("MOCK_SEND_DELAY must be a non-negative duration")
		}
		cfg.SendDelay = d
	}
	if v := os.Getenv("MOCK_FAILURE_RATE"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || math.IsNaN(r) || math.IsInf(r, 0) || r < 0 || r > 1 {
			return MockConfig{}, fmt.Errorf("MOCK_FAILURE_RATE must be a number from 0 to 1")
		}
		cfg.FailureRate = r
	}
	return cfg, nil
}

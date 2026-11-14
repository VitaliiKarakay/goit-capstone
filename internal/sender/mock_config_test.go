package sender

import (
	"os"
	"testing"
)

func TestLoadMockConfigDefaults(t *testing.T) {
	t.Setenv("MOCK_SEND_DELAY", "")
	t.Setenv("MOCK_FAILURE_RATE", "")
	cfg, err := LoadMockConfig()
	if err != nil || cfg.SendDelay != 0 || cfg.FailureRate != 0 {
		t.Fatalf("%+v err=%v", cfg, err)
	}
}

func TestLoadMockConfigInvalid(t *testing.T) {
	for _, tc := range []struct {
		delay string
		rate  string
	}{
		{"-1s", ""},
		{"nope", ""},
		{"", "-0.1"},
		{"", "2"},
		{"", "NaN"},
		{"", "Inf"},
	} {
		t.Run(tc.delay+tc.rate, func(t *testing.T) {
			if tc.delay != "" {
				t.Setenv("MOCK_SEND_DELAY", tc.delay)
			} else {
				os.Unsetenv("MOCK_SEND_DELAY")
			}
			if tc.rate != "" {
				t.Setenv("MOCK_FAILURE_RATE", tc.rate)
			} else {
				os.Unsetenv("MOCK_FAILURE_RATE")
			}
			if _, err := LoadMockConfig(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

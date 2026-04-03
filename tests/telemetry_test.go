package tests

import (
	"testing"

	"github.com/iamitprakash/GMP-go/pkg/telemetry"
)

func TestTelemetryMetrics(t *testing.T) {
	// The expvar registry is global, so we simply verify the structs are addressable
	// and increment accurately without panic.
	
	telemetry.TasksSubmitted.Add(1)
	if telemetry.TasksSubmitted.Value() <= 0 {
		t.Errorf("Expected TasksSubmitted to track properly, got: %d", telemetry.TasksSubmitted.Value())
	}

	telemetry.ActiveMs.Add(1)
	if telemetry.ActiveMs.Value() <= 0 {
		t.Errorf("Expected ActiveMs to increment")
	}

	telemetry.ActiveMs.Add(-1)
	// Success if no panic occurred across concurrent state atomic gauges.
}

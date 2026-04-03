package telemetry

import (
	"expvar"
	"net/http"
	_ "net/http/pprof" // Embeds standard Go deep CPU profiling and Flame Graph routes seamlessly (/debug/pprof)
)

// Exported metrics for real-time profiling via expvar.
var (
	TasksSubmitted = expvar.NewInt("gmp_tasks_submitted")
	TasksExecuted  = expvar.NewInt("gmp_tasks_executed")
	TasksStolen    = expvar.NewInt("gmp_tasks_stolen")
	Handoffs       = expvar.NewInt("gmp_handoffs")
	ActiveMs       = expvar.NewInt("gmp_active_ms")
	IdleMs         = expvar.NewInt("gmp_idle_ms")
)

// StartServer starts a debug HTTP server to expose metrics at /debug/vars and rendering.
func StartServer(addr string) {
	initDashboard() // Attaches Web UI to /dashboard
	go func() {
		// Non-blocking serving of standard expvar metrics
		_ = http.ListenAndServe(addr, nil)
	}()
}

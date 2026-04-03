package telemetry

import "net/http"

func initDashboard() {
	http.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(dashboardHTML))
	})
}

const dashboardHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>GMP-Go Live Telemetry</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: 'Inter', sans-serif; background-color: #0d1117; color: #c9d1d9; margin: 0; padding: 20px; }
        h1 { text-align: center; color: #58a6ff; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; max-width: 1200px; margin: 0 auto; }
        .card { background: #161b22; border-radius: 10px; padding: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); border: 1px solid #30363d;}
        .stat { font-size: 2em; font-weight: bold; color: #3fb950; display: block; margin-top: 10px;}
        canvas { max-width: 100%; height: 300px; }
    </style>
</head>
<body>
    <h1>GMP-Go Live Telemetry Metrics</h1>
    <div class="grid">
        <div class="card">
            <h3>System Throughput</h3>
            <span id="rateLabel" class="stat">0 tasks/sec</span>
            <canvas id="throughputChart"></canvas>
        </div>
        <div class="card">
            <h3>Machine States (Threads)</h3>
            <canvas id="threadsChart"></canvas>
        </div>
        <div class="card">
            <h3>Total Lifetime Operations</h3>
            <p>Executed: <span id="execLabel" class="stat">0</span></p>
            <p>Stolen: <span id="stealLabel" class="stat" style="color: #bc8cff">0</span></p>
            <p>Handoffs: <span id="handoffLabel" class="stat" style="color: #f85149">0</span></p>
        </div>
    </div>

    <script>
        const ctxThru = document.getElementById('throughputChart').getContext('2d');
        const throughputChart = new Chart(ctxThru, {
            type: 'line',
            data: { labels: [], datasets: [{ label: 'Tasks Executed', borderColor: '#3fb950', backgroundColor: 'rgba(63, 185, 80, 0.2)', data: [], fill: true, tension: 0.4 }] },
            options: { responsive: true, animation: { duration: 0 }, scales: { x: { display: false }, y: { beginAtZero: true } } }
        });

        const ctxThreads = document.getElementById('threadsChart').getContext('2d');
        const threadsChart = new Chart(ctxThreads, {
            type: 'bar',
            data: { labels: ['Active M-Threads', 'Idle M-Threads'], datasets: [{ label: 'Count', backgroundColor: ['#58a6ff', '#8b949e'], data: [0, 0] }] },
            options: { responsive: true, animation: { duration: 500 }, scales: { y: { beginAtZero: true } } }
        });

        let lastExecuted = 0;
        let lastTime = Date.now();

        async function fetchMetrics() {
            try {
                const res = await fetch('/debug/vars');
                const data = await res.json();

                const exec = parseInt(data.gmp_tasks_executed || 0);
                const stolen = parseInt(data.gmp_tasks_stolen || 0);
                const handoffs = parseInt(data.gmp_handoffs || 0);
                const active = parseInt(data.gmp_active_ms || 0);
                const idle = parseInt(data.gmp_idle_ms || 0);

                const now = Date.now();
                const diffTime = (now - lastTime) / 1000;
                let rate = 0;
                if (lastExecuted > 0 && diffTime > 0) {
                    rate = Math.round((exec - lastExecuted) / diffTime);
                }
                lastExecuted = exec;
                lastTime = now;

                document.getElementById('execLabel').innerText = exec.toLocaleString();
                document.getElementById('stealLabel').innerText = stolen.toLocaleString();
                document.getElementById('handoffLabel').innerText = handoffs.toLocaleString();
                document.getElementById('rateLabel').innerText = Math.max(0, rate).toLocaleString() + " tasks/sec";

                const ts = new Date().toLocaleTimeString();
                if (throughputChart.data.labels.length > 20) {
                    throughputChart.data.labels.shift();
                    throughputChart.data.datasets[0].data.shift();
                }
                throughputChart.data.labels.push(ts);
                throughputChart.data.datasets[0].data.push(rate);
                throughputChart.update();

                threadsChart.data.datasets[0].data = [active, idle];
                threadsChart.update();

            } catch(e) { console.error("Telemetry failed fetching", e); }
        }

        setInterval(fetchMetrics, 1000);
    </script>
</body>
</html>
`

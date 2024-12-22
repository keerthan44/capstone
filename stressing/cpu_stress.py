import multiprocessing
import time
import psutil
import signal
import sys
import os
import threading
from flask import Flask, jsonify
from statistics import mean

def ignore_signals():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

class CPUStressor:
    def __init__(self, target_percent: float = 80.0):
        self.target_percent = target_percent
        self.running = True
        self.process = psutil.Process(os.getpid())
        self.worker_processes = []
        self.num_cores = multiprocessing.cpu_count()
        self.stress_thread = None
        self.load_lock = threading.Lock()
        self.current_iterations = 10000
        self.max_iterations = 50000
        self.min_iterations = 1000

    def stress_cpu(self) -> None:
        while self.running:
            x = 2**32
            for _ in range(self.current_iterations):
                x = (x * x) % 2**32
            time.sleep(0.0005)

    def get_own_cpu_percent(self) -> float:
        try:
            samples = []
            for _ in range(3):
                total_percent = self.process.cpu_percent(interval=0.1)
                for proc in self.worker_processes:
                    if proc.is_alive():
                        try:
                            proc_psutil = psutil.Process(proc.pid)
                            total_percent += proc_psutil.cpu_percent(interval=0.1)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue
                samples.append(total_percent)
            return mean(samples) if samples else 0.0
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0

    def adjust_load(self) -> None:
        psutil.cpu_percent(interval=0.1)
        self.process.cpu_percent(interval=0.1)

        initial_processes = self.num_cores
        self.worker_processes = [
            multiprocessing.Process(target=self.stress_cpu) for _ in range(initial_processes)
        ]

        for p in self.worker_processes:
            p.start()

        recent_cpu = []

        while self.running:
            with self.load_lock:
                own_cpu_percent = self.get_own_cpu_percent()
                recent_cpu.append(own_cpu_percent)
                if len(recent_cpu) > 5:
                    recent_cpu.pop(0)

                avg_cpu = mean(recent_cpu) if recent_cpu else own_cpu_percent
                print(f"Current CPU usage: {avg_cpu:.1f}% (Target: {self.target_percent}%)")

                if avg_cpu < self.target_percent - 2:
                    if self.current_iterations < self.max_iterations:
                        self.current_iterations = min(int(self.current_iterations * 1.2), self.max_iterations)
                    else:
                        new_process = multiprocessing.Process(target=self.stress_cpu)
                        new_process.start()
                        self.worker_processes.append(new_process)

                elif avg_cpu > self.target_percent + 2:
                    for i in range(len(self.worker_processes) - 1, -1, -1):
                        p = self.worker_processes[i]
                        if p.is_alive():
                            p.terminate()
                            p.join(timeout=1)
                            self.worker_processes.pop(i)
                            break
                    else:
                        self.current_iterations = max(int(self.current_iterations * 0.8), self.min_iterations)

            time.sleep(0.2)

    def start(self):
        self.running = True
        self.stress_thread = threading.Thread(target=self.adjust_load)
        self.stress_thread.daemon = True
        self.stress_thread.start()
        return {"status": "started", "target_cpu": self.target_percent}

    def stop(self):
        self.running = False
        self.cleanup()
        return {"status": "stopped"}

    def cleanup(self) -> None:
        self.running = False
        for i in range(len(self.worker_processes) - 1, -1, -1):
            p = self.worker_processes[i]
            if p.is_alive():
                try:
                    p.terminate()
                    p.join(timeout=1)
                except Exception:
                    pass
            self.worker_processes.pop(i)

# Create Flask app and CPU stressor instance
app = Flask(__name__)
stressor = CPUStressor(target_percent=80.0)

@app.route('/start', methods=['POST'])
def start_stress():
    return jsonify(stressor.start())

@app.route('/stop', methods=['POST'])
def stop_stress():
    return jsonify(stressor.stop())

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify({
        "running": stressor.running,
        "current_cpu_usage": stressor.get_own_cpu_percent(),
        "target_cpu_usage": stressor.target_percent
    })

if __name__ == "__main__":
    def signal_handler(signum, frame):
        if stressor.running:
            print(f"\nSignal {signum} received. Shutting down gracefully...")
            stressor.cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("Starting Flask server on port 5000...")
    print("Available endpoints:")
    print("  POST /start  - Start CPU stress")
    print("  POST /stop   - Stop CPU stress")
    print("  GET  /status - Get current status")

    app.run(host='0.0.0.0', port=5000, threaded=True)

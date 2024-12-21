import multiprocessing
import time
import psutil
import signal
import sys
import os
import threading
import subprocess
from typing import List
from flask import Flask, jsonify, request
from statistics import mean


class CPUStressor:
    def __init__(self, target_percent: float = 80.0):
        self.target_percent = target_percent
        self.running = True
        self.process = psutil.Process(os.getpid())
        self.worker_processes: List[multiprocessing.Process] = []
        self.num_cores = multiprocessing.cpu_count()
        self.stress_thread = None
        self.load_lock = threading.Lock()
        self.current_iterations = 10000  # Starting point for iterations
        self.max_iterations = 50000     # Maximum allowed iterations
        self.min_iterations = 1000      # Minimum allowed iterations
        
    def stress_cpu(self) -> None:
        """Function to consume CPU cycles with dynamic intensity"""
        while self.running:
            x = 2**32
            for _ in range(self.current_iterations):
                x = (x * x) % 2**32
            time.sleep(0.0005)  # Small consistent sleep

    def get_own_cpu_percent(self) -> float:
        """Get CPU usage with moving average"""
        try:
            samples = []
            for _ in range(3):  # Take 3 samples
                total_percent = self.process.cpu_percent(interval=0.1)
                for proc in self.worker_processes:
                    if proc.is_alive():
                        try:
                            proc_psutil = psutil.Process(proc.pid)
                            total_percent += proc_psutil.cpu_percent(interval=0.1)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue
                samples.append(total_percent)
            
            # Return moving average
            return mean(samples) if samples else 0.0
            
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0

    def adjust_load(self) -> None:
        """Adjusts CPU load to maintain target usage"""
        # Initialize CPU monitoring
        psutil.cpu_percent(interval=0.1)
        self.process.cpu_percent(interval=0.1)
        
        # Start with one process per core
        initial_processes = self.num_cores
        self.worker_processes = [
            multiprocessing.Process(target=self.stress_cpu)
            for _ in range(initial_processes)
        ]
        
        for p in self.worker_processes:
            p.start()

        # Keep track of recent CPU percentages
        recent_cpu = []
        
        while self.running:
            with self.load_lock:
                own_cpu_percent = self.get_own_cpu_percent()
                recent_cpu.append(own_cpu_percent)
                if len(recent_cpu) > 5:  # Keep last 5 readings
                    recent_cpu.pop(0)
                
                avg_cpu = mean(recent_cpu) if recent_cpu else own_cpu_percent
                print(f"Current CPU usage: {avg_cpu:.1f}% (Target: {self.target_percent}%)")
                
                # Adjust process count and iteration count based on CPU usage
                if avg_cpu < self.target_percent - 2:  # Below target range
                    if self.current_iterations < self.max_iterations:
                        self.current_iterations = min(self.current_iterations * 1.2, self.max_iterations)
                    else:
                        new_process = multiprocessing.Process(target=self.stress_cpu)
                        new_process.start()
                        self.worker_processes.append(new_process)
                
                elif avg_cpu > self.target_percent + 2:  # Above target range
                    if len(self.worker_processes) > 1:
                        p = self.worker_processes.pop()
                        if p.is_alive():
                            p.terminate()
                            p.join()
                    else:
                        self.current_iterations = max(self.current_iterations * 0.8, self.min_iterations)
                
                # Fine-tune within target range (80 ± 5)
                elif avg_cpu > self.target_percent + 5:  # Too high, reduce load
                    self.current_iterations = max(self.current_iterations * 0.9, self.min_iterations)
                    
                elif avg_cpu < self.target_percent - 5:  # Too low, increase load
                    if self.current_iterations < self.max_iterations:
                        self.current_iterations = min(self.current_iterations * 1.1, self.max_iterations)
            
        time.sleep(0.2)  # Reduce the sleep interval for more responsive adjustments

    def setup_firewall(self):
        """Ensure port 5000 is open in the firewall."""
        try:
            subprocess.run(["sudo", "ufw", "allow", "5000"], check=True)
            subprocess.run(["sudo", "ufw", "status"], check=True)
            print("Firewall setup completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error during firewall setup: {e}")

    def start(self):
        """Start the CPU stressor in a separate thread"""
        self.running = True
        self.stress_thread = threading.Thread(target=self.adjust_load)
        self.stress_thread.daemon = True
        self.stress_thread.start()
        return {"status": "started", "target_cpu": self.target_percent}

    def stop(self):
        """Stop the CPU stressor"""
        self.running = False
        if self.stress_thread:
            self.stress_thread.join(timeout=2)
        self.cleanup()
        return {"status": "stopped"}

    def cleanup(self) -> None:
        """Clean up all worker processes"""
        self.running = False
        for p in self.worker_processes:
            if p.is_alive():
                p.terminate()
                p.join()

# Create Flask app and CPU stressor instance
app = Flask(__name__)
stressor = CPUStressor(target_percent=80.0)

@app.route('/start', methods=['POST'])
def start_stress():
    """Start the CPU stress test"""
    return jsonify(stressor.start())

@app.route('/stop', methods=['POST'])
def stop_stress():
    """Stop the CPU stress test"""
    return jsonify(stressor.stop())

@app.route('/status', methods=['GET'])
def get_status():
    """Get current CPU usage status"""
    return jsonify({
        "running": stressor.running,
        "current_cpu_usage": stressor.get_own_cpu_percent(),
        "target_cpu_usage": stressor.target_percent
    })

if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print("\nShutting down gracefully...")
        stressor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Starting Flask server on port 5000...")
    print("Available endpoints:")
    print("  POST /start  - Start CPU stress")
    print("  POST /stop   - Stop CPU stress")
    print("  GET  /status - Get current status")
    
    # Run Flask app with threading enabled
    app.run(host='0.0.0.0', port=5000, threaded=True)

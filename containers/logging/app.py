from flask import Flask, request, jsonify
import redis
import os
import sys
import csv
import threading
import queue
import time

redis_ip = os.environ.get("REDIS_IP_ADDRESS")

app = Flask(__name__)
redis_client = redis.Redis(host=redis_ip, port=6379)
start_time = None
csv_file = 'request_logs.csv'

# A thread-safe queue to store logs for batch processing
log_queue = queue.Queue()

# Ensure the CSV file has headers if it doesn't exist
if not os.path.exists(csv_file):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['timestamp_actual', 'timestamp_sent', 'timestamp_received', 'dm', 'um', 'communication_type', 'time_delay'])

# Background thread to handle writing log data to the CSV file
def log_writer():
    while True:
        log_entries = []
        try:
            # Collect log entries in batches
            while len(log_entries) < 100 and not log_queue.empty():
                log_entries.append(log_queue.get(timeout=1))
            
            # Append the batch to the CSV file
            if log_entries:
                with open(csv_file, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerows(log_entries)

        except queue.Empty:
            # If the queue is empty, wait for new data
            pass
        except Exception as e:
            print(f"Error writing logs: {e}", file=sys.stderr)
        time.sleep(1)  # Adjust the sleep time based on your desired batch size/write frequency


@app.route('/logs', methods=['POST'])
def log_data():
    global start_time
    if not start_time:
        start_time = int(redis_client.get('start_time')) // 1_000_000
    
    data = request.get_json()
    timestamp_received = int(data['timestamp_received']) - start_time
    timestamp_sent = int(data['timestamp_sent']) - start_time
    timestamp_actual = int(data['timestamp_actual']) - start_time
    time_delay = timestamp_received - timestamp_sent

    # Log the data to stderr
    print(f"[{timestamp_actual}][{timestamp_sent}][{timestamp_received}] {data['dm']} received request from {data['um']} with communication_type {data['communication_type']}. Time delay: {time_delay} ms. Start time: {start_time}", file=sys.stderr)
    
    # Add the log entry to the queue
    log_queue.put([timestamp_actual, timestamp_sent, timestamp_received, data['dm'], data['um'], data['communication_type'], time_delay])
    
    return jsonify({"message": "Data received"}), 200

if __name__ == '__main__':
    # Start the background logging thread
    threading.Thread(target=log_writer, daemon=True).start()
    app.run(host='0.0.0.0', port=80)

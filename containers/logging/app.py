from flask import Flask, request, jsonify
import redis
import os
import sys

redis_ip = os.environ.get("REDIS_IP_ADDRESS")

app = Flask(__name__)
redis_client = redis.Redis(host=redis_ip, port=6379)
start_time = None

@app.route('/logs', methods=['POST'])
def log_data():
    global start_time
    if not start_time:
        start_time = int(redis_client.get('start_time')) // 1_000_000
    data = request.get_json()
    timestamp_received = int(data['timestamp_received']) - start_time
    timestamp_sent = int(data['timestamp_sent']) - start_time
    timestamp_actual = int(data['timestamp_actual']) 
    print(f"[{timestamp_actual}][{timestamp_sent}][{timestamp_received}] {data['dm']} received request from {data['um']} with communication_type {data['communication_type']}. Time delay: {timestamp_received - timestamp_sent} ms. Start time: {start_time}", file=sys.stderr)
    return jsonify({"message": "Data received"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)

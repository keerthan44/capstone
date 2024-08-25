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
        start_time = int(redis_client.get('start_time'))
    data = request.get_json()
    print(f"[{data['timestamp']}] {data['dm']} received request from {data['um']} with communication_type {data['communication_type']}. Start time: {start_time}", file=sys.stderr)
    return jsonify({"message": "Data received"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)

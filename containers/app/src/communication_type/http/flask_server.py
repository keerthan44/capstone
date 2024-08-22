from flask import Flask, request, jsonify
import requests
import os
import redis
import sys

app = Flask(__name__)

# Environment variables
container_name = os.environ.get("CONTAINER_NAME")

@app.route('/', methods=['POST'])
def home():
    data = request.get_json()
    # log
    try:
        response = requests.post(f"http://logging-service/logs", 
                                    json={"timestamp": data["timestamp"], 
                                          "dm": container_name,
                                          "um": data['um']
                                          })
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact logging_capstone: {e}")

    return jsonify({"status": "success"}), 200

def start_flask():
    app.run(host='0.0.0.0', port=80)

def make_http_call(data):
    try:
        response = requests.post(f"http://{data['dm_service']}-service/", 
                                json={"timestamp": data['timestamp'], "um": container_name})
        print(f"Contacted {data['dm_service']} with communication_type {data['communication_type']}: {response.text}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact {data['dm_service']}: {e}", file=sys.stderr)


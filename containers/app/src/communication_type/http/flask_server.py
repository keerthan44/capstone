from flask import Flask, request, jsonify
import requests
import os
import redis
import sys
import multiprocessing

app = Flask(__name__)

# Environment variables
container_name = os.environ.get("CONTAINER_NAME")

@app.route('/', methods=['POST'])
def home():
    data = request.get_json()
    # log
    dm = container_name
    um = data['um']
    timestamp = data['timestamp']
    communication_type = data['communication_type']
    make_http_call_to_logging_server(um, dm, timestamp, communication_type)
    return jsonify({"status": "success"}), 200

def start_flask_process():
    """Start the Flask application in a new process."""
    def run_flask():
        app.run(host='0.0.0.0', port=80)
    
    # Create a new process for the Flask server
    flask_process = multiprocessing.Process(target=run_flask)
    flask_process.start()
    print("Flask server started in a new process.")
    
    return flask_process

def make_http_call(data):
    try:
        response = requests.post(f"http://{data['dm_service']}-service/", 
                                json={"timestamp": data['timestamp'], "um": container_name, "communication_type": data['communication_type']})
        print(f"Contacted {data['dm_service']} with communication_type {data['communication_type']}: {response.text}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact {data['dm_service']}: {e}", file=sys.stderr)

def make_http_call_to_logging_server(um, dm, timestamp, communication_type):
    try:
        response = requests.post(f"http://logging-service/logs", 
                                json={"timestamp": timestamp, 
                                      "dm": dm,
                                      "um": um,
                                      "communication_type": communication_type
                                      })
        print(f"Contacted logging_capstone with communication_type {communication_type}: {response.text}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact logging_capstone: {e}", file=sys.stderr)


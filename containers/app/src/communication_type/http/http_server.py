from flask import Flask, request, jsonify
import os
import multiprocessing
from .http_client import make_http_call_to_logging_server

app = Flask(__name__)

# Environment variables
container_name = os.environ.get("CONTAINER_NAME")

@app.route('/', methods=['POST'])
def home():
    data = request.get_json()
    # log
    dm = container_name
    um = data['um']
    timestamp_sent = data['timestamp_sent']
    communication_type = data['communication_type']
    make_http_call_to_logging_server(um, dm, timestamp_sent, communication_type)
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

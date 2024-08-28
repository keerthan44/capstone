import time
from quart import Quart, request, jsonify
import os
import multiprocessing
from .http_client import make_http_call_to_logging_server

app = Quart(__name__)

# Environment variables
container_name = os.environ.get("CONTAINER_NAME")

@app.route('/', methods=['POST'])
async def home():
    timestamp_received = str(time.time_ns() // 1_000_000)
    data = await request.get_json()
    data['dm'] = container_name
    data['timestamp_received'] = timestamp_received
    print(data)
    await make_http_call_to_logging_server(data)
    return jsonify({"status": "success"}), 200

def start_flask_process():
    """Start the Quart application in a new process."""
    def run_quart():
        app.run(host='0.0.0.0', port=80)
    
    # Create a new process for the Quart server
    quart_process = multiprocessing.Process(target=run_quart)
    quart_process.start()
    print("Quart server started in a new process.")
    
    return quart_process

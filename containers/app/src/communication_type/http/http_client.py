import sys
import requests
import requests
import os
import time

container_name = os.environ.get("CONTAINER_NAME")
def make_http_call(data):
    try:
        json_body = {
            "timestamp_sent": data['timestamp_sent'], 
            "um": container_name, 
            "communication_type": data['communication_type'],
            'timestamp_actual': data['timestamp_actual']
                     }
        response = requests.post(f"http://{data['dm_service']}-service/", 
                                json=json_body)
        print(f"Contacted {data['dm_service']} with communication_type {data['communication_type']}: {response.text}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact {data['dm_service']}: {e}", file=sys.stderr)

def make_http_call_to_logging_server(data):
    try:
        response = requests.post(f"http://logging-service/logs", json=data)
                                    
        print(f"Contacted logging_capstone with communication_type {data['communication_type']}: {response.text}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact logging_capstone: {e}", file=sys.stderr)


import sys
import requests
import requests
import os

container_name = os.environ.get("CONTAINER_NAME")
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


import sys
import requests
import requests
import os
import time

container_name = os.environ.get("CONTAINER_NAME")
def make_http_call(data):
    try:
        response = requests.post(f"http://{data['dm_service']}-service/", 
                                json={"timestamp_sent": data['timestamp_sent'], "um": container_name, "communication_type": data['communication_type']})
        print(f"Contacted {data['dm_service']} with communication_type {data['communication_type']}: {response.text}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact {data['dm_service']}: {e}", file=sys.stderr)

def make_http_call_to_logging_server(um, dm, timestamp_sent, communication_type):
    try:
        response = requests.post(f"http://logging-service/logs", 
                                json={"timestamp_sent": timestamp_sent, 
                                      "dm": dm,
                                      "um": um,
                                      "communication_type": communication_type,
                                      'timestamp_received': str(time.time_ns() // 1_000_000)
                                      })
        print(f"Contacted logging_capstone with communication_type {communication_type}: {response.text}", file=sys.stderr)
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact logging_capstone: {e}", file=sys.stderr)


import requests
import threading
import time
import json
import docker
import os
import redis
import sys
from communication_type.http.flask_server import start_flask, make_http_call 

container_name = os.environ.get("CONTAINER_NAME")
redis_ip = os.environ.get("REDIS_IP_ADDRESS")
container_job = os.environ.get("CONTAINER_JOB")
namespace = os.environ.get("NAMESPACE")
redis_client = redis.StrictRedis(host=f'{redis_ip}', port=6379)
start_time = ''

def get_timestamp_to_call(start_time, calls_list):
    timestamp = (time.time_ns() - start_time) // 1_000_000   # Convert to milliseconds
    timestamps = []
    for _ in range(0, len(calls_list)):
        if calls_list[0] > timestamp:
            break
        timestamps.append(calls_list.pop(0))
    return timestamp, timestamps

def get_containers_to_call(calls, timestamps):
    containers = []
    for tempTimeStamp in timestamps:
        containers.extend(calls[str(tempTimeStamp)])
    return containers

def call_containers(containers, timestamp):
    print(f"Time: {timestamp}", file=sys.stderr)
    for container in containers:
        dm_service = container['dm_service']
        communication_type = container['communication_type']
        print(f"sent request to {dm_service} with communication_type {communication_type}", file=sys.stderr)
        match communication_type:
            case 'mc':
                try:
                    response = requests.post(f"http://{dm_service}-service/", 
                                            json={"timestamp": timestamp, "um": container_name})
                    print(f"Contacted {dm_service} with communication_type {communication_type}: {response.text}", file=sys.stderr)
                except requests.exceptions.RequestException as e:
                    print(f"Failed to contact {dm_service}: {e}", file=sys.stderr)
            case _:
                make_http_call({**container, 'timestamp': timestamp})

def sleep_according_to_call_list(calls_list, start_time):
    if calls_list:
        sleep_time_sec = (calls_list[0] - (time.time_ns() - start_time) // 1_000_000) / 1_000
        if sleep_time_sec < 0:
            time.sleep(0.00001)
        else:
            time.sleep(sleep_time_sec)
        return 'slept'
    else:
        return 'not_slept'


def contact_containers(calls):
    calls_list = list(map(int, calls.keys()))
    calls_list.sort()
    print(calls_list, file=sys.stderr)
    
    while True:
        print("Waiting for start_time")
        if redis_client.exists('start_time'):
            print("start_time found")
            start_time = int(redis_client.get('start_time'))
            break
        time.sleep(0.000001)
    
    if container_job == '0':
        while True:
            timestamp, timestamps = get_timestamp_to_call(start_time, calls_list)
            print(timestamp, timestamps, file=sys.stderr)
            containers = get_containers_to_call(calls, timestamps)
            if containers:
                call_containers(containers, timestamp)
            if sleep_according_to_call_list(calls_list, start_time) == 'not_slept':
                break

    else:    
        stop_event = threading.Event()
        
        def background_task():
            while not stop_event.is_set():
                # Simulate background task
                print("Background task is running...", file=sys.stderr)
                time.sleep(1)
        
        # Start the background task in a separate thread
        bg_thread = threading.Thread(target=background_task)
        bg_thread.start()
        
        while True:
            timestamp, timestamps = get_timestamp_to_call(start_time, calls_list)
            containers = get_containers_to_call(calls, timestamps)
            if containers:
                # Pause the background task
                stop_event.set()
                bg_thread.join()
                
                call_containers(containers, timestamp)
                
                # Reset the stop event and restart the background task
                stop_event.clear()
                bg_thread = threading.Thread(target=background_task)
                bg_thread.start()
            
            if sleep_according_to_call_list(calls_list, start_time) == 'not_slept':
                break

if __name__ == "__main__":
    # Load the contact data from a JSON file
    while True:
        print("Waiting for calls.json")
        if os.path.exists('calls.json'):
            print("calls.json found")
            with open('calls.json') as f:
                calls = json.load(f)
            break
        time.sleep(1)
    print(calls)
    if calls:
        threading.Thread(target=contact_containers, args=(calls,)).start()
    start_flask()
    
import requests
import threading
import time
import json
import docker
import os
import redis
import sys
from communication_type.http.flask_server import start_flask_process, make_http_call 
from communication_type.kafka.kafka_related import produce_kafka_messages, start_kafka_consumer_process

CONTAINER_NAME = os.environ.get("CONTAINER_NAME")
REDIS_IP = os.environ.get("REDIS_IP_ADDRESS")
CONTAINER_JOB = os.environ.get("CONTAINER_JOB")
NAMESPACE = os.environ.get("NAMESPACE")

redis_client = redis.StrictRedis(host=f'{REDIS_IP}', port=6379)
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
        json_data = {**container, 'timestamp': timestamp, "um": CONTAINER_NAME}
        print(f"sent request to {dm_service} with communication_type {communication_type}", file=sys.stderr)
        match communication_type:
            case 'async':
                produce_kafka_messages(NAMESPACE, 'kafka-instance', 'kafka', dm_service, json_data)
            case _:
                make_http_call(json_data)

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
        if redis_client.exists('start_time'):
            print("start_time found")
            start_time = int(redis_client.get('start_time'))
            break
        time.sleep(0.000001)
    
    if CONTAINER_JOB == '0':
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
    kafka_process = start_kafka_consumer_process(NAMESPACE, 'kafka-instance', 'kafka', f'{CONTAINER_NAME}-group', CONTAINER_NAME, CONTAINER_NAME)
    flask_process = start_flask_process()
    

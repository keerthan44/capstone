import threading
import time
import json
import os
import redis
import sys
import asyncio

from communication_type.http.http_server import start_flask_process
from communication_type.http.http_client import make_http_call

from communication_type.kafka.kafka_producer import produce_kafka_messages
from communication_type.kafka.kafka_consumer import start_kafka_consumer_process

from communication_type.rpc.rpc_client import contact_rpc_server
from communication_type.rpc.rpc_server import run_grpc_server_process

CONTAINER_NAME = os.environ.get("CONTAINER_NAME")
REDIS_IP = os.environ.get("REDIS_IP_ADDRESS")
CONTAINER_JOB = os.environ.get("CONTAINER_JOB")
NAMESPACE = os.environ.get("NAMESPACE")
KAFKA_REPLICAS = int(os.environ.get("KAFKA_REPLICAS"))
POD_NAME = os.environ.get("POD_NAME")

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


async def call_containers(containers, timestamp, start_time):
    print(f"Time: {timestamp}", file=sys.stderr)
    timestamp_actual = str(start_time // 1_000_000 + timestamp)
    tasks = []

    for container in containers:
        dm_service = container['dm_service']
        communication_type = container['communication_type']
        json_data = {
            'timestamp_actual': timestamp_actual,
            'dm_service': dm_service, 
            'communication_type': communication_type, 
            'timestamp_sent': str(time.time_ns() // 1_000_000), 
            "um": CONTAINER_NAME
            }
        print(f"Sent request to {dm_service} with communication_type {communication_type}", file=sys.stderr)
        try:
            match communication_type:
                case 'mq':
                    tasks.append(asyncio.create_task(produce_kafka_messages(NAMESPACE, 'kafka-instance', 'kafka', dm_service, json_data, KAFKA_REPLICAS)))
                case 'rpc':
                    tasks.append(asyncio.create_task(contact_rpc_server(json_data)))
                case _:
                    tasks.append(asyncio.create_task(make_http_call(json_data)))
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    if tasks:
        await asyncio.gather(*tasks)


async def sleep_according_to_call_list(calls_list, start_time):
    if calls_list:
        sleep_time_sec = (calls_list[0] - (time.time_ns() - start_time) // 1_000_000) / 1_000
        if sleep_time_sec < 0:
            await asyncio.sleep(0.00001)
        else:
            await asyncio.sleep(sleep_time_sec)
        return 'slept'
    else:
        return 'not_slept'


async def contact_containers(calls):
    calls_list = list(map(int, calls.keys()))
    calls_list.sort()
    print(calls_list, file=sys.stderr)
    
    while True:
        if redis_client.exists('start_time'):
            print("start_time found")
            start_time = int(redis_client.get('start_time'))
            break
        await asyncio.sleep(0.000001)
    if CONTAINER_JOB == '0':
        while True:
            timestamp, timestamps = get_timestamp_to_call(start_time, calls_list)
            print(timestamp, timestamps, file=sys.stderr)
            containers = get_containers_to_call(calls, timestamps)
            if containers:
                await call_containers(containers, timestamp, start_time)
            if await sleep_according_to_call_list(calls_list, start_time) == 'not_slept':
                break

    else:    
        stop_event = threading.Event()
        # Function to compute Pi using the Leibniz series
        def compute_pi(terms: int) -> float:
            pi = 0.0
            for i in range(terms):
                pi += ((-1) ** i) / (2 * i + 1)  # Leibniz series formula
            return 4 * pi
            
        def background_task(terms=100000):
            while not stop_event.is_set():
                # Compute Pi
                pi_value = compute_pi(terms)
                
                # Simulate background task and print Pi value
                print(f"Background task is running... Computed Pi: {pi_value}", file=sys.stderr)
            
        # Start the background task in a separate thread
        bg_thread = threading.Thread(target=background_task, daemon=True)
        bg_thread.start()
        
        while True:
            timestamp, timestamps = get_timestamp_to_call(start_time, calls_list)
            containers = get_containers_to_call(calls, timestamps)
            if containers:
                # Pause the background task
                stop_event.set()
                bg_thread.join()
                
                await call_containers(containers, timestamp, start_time)
                
                # Reset the stop event and restart the background task
                stop_event.clear()
                bg_thread = threading.Thread(target=background_task)
                bg_thread.start()
            
            if await sleep_according_to_call_list(calls_list, start_time) == 'not_slept':
                break


if __name__ == "__main__":
    # Load the contact data from a JSON file
    kafka_process = start_kafka_consumer_process(NAMESPACE, 'kafka-instance', 'kafka', f'{CONTAINER_NAME}-group', CONTAINER_NAME, CONTAINER_NAME, KAFKA_REPLICAS)
    flask_process = start_flask_process()
    grpc_process = run_grpc_server_process()
    while True:
        print("Waiting for calls.json")
        if os.path.exists('data/calls.json'):
            print("calls.json found")
            with open('data/calls.json') as f:
                calls = json.load(f)[POD_NAME]
            break
        time.sleep(1)
    print(calls)
    asyncio.run(contact_containers(calls))

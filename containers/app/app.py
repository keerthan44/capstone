from flask import Flask, request, jsonify
import requests
import threading
import time
import json
import docker
import os
import redis
import sys

app = Flask(__name__)

container_name = os.environ.get("CONTAINER_NAME")
redis_ip = os.environ.get("REDIS_IP_ADDRESS")
container_job = os.environ.get("CONTIANER_JOB")
redis_client = redis.StrictRedis(host=redis_ip, port=6379)
start_time = ''



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
        time.sleep(1)
    if container_job == '0':
        while True:
            timestamp = time.time_ns() // 1000 - start_time
            timestamps = []
            for _ in range(0, len(calls_list)):
                if calls_list[0] > timestamp:
                    break
                timestamps.append(calls_list.pop(0))
            containers = []
            for tempTimeStamp in timestamps:
                containers.extend(calls[str(tempTimeStamp)])
            if containers:
                for container in containers:
                    try:
                        print(f"sent request to {container}", file=sys.stderr)
                        response = requests.post(f"http://{container[:5]}/", 
                                                json={"timestamp": timestamp, "um": container_name})
                        print(f"Contacted {container}: {response.text}", file=sys.stderr)
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to contact {container[:5]}: {e}", file=sys.stderr)
            if calls_list:
                sleep_time = calls_list[0] - (time.time_ns() // 1000 - start_time)
                if sleep_time < 0:
                    time.sleep(0.00001)
                else:
                    time.sleep(sleep_time / 1000000)
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
            timestamp = time.time_ns() // 1000 - start_time
            timestamps = []
            for _ in range(0, len(calls_list)):
                if calls_list[0] > timestamp:
                    break
                timestamps.append(calls_list.pop(0))
            print(timestamps)
            containers = []
            for tempTimeStamp in timestamps:
                containers.extend(calls[str(tempTimeStamp)])
            print(containers)
            
            if containers:
                # Pause the background task
                stop_event.set()
                bg_thread.join()
                
                for container in containers:
                    try:
                        print(f"sent request to {container}", file=sys.stderr)
                        response = requests.post(f"http://{container[:5]}/", 
                                                json={"timestamp": timestamp, "um": container_name})
                        print(f"Contacted {container}: {response.text}", file=sys.stderr)
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to contact {container[:5]}: {e}", file=sys.stderr)
                
                # Reset the stop event and restart the background task
                stop_event.clear()
                bg_thread = threading.Thread(target=background_task)
                bg_thread.start()
            
            if calls_list:
                sleep_time = calls_list[0] - (time.time_ns() // 1000 - start_time)
                time.sleep(sleep_time)
            else:
                break

@app.route('/', methods=['POST'])
def home():
    data = request.get_json()
    # log
    print(data, start_time)
    try:
        response = requests.post(f"http://logging_capstone/logs", 
                                    json={"timestamp": data["timestamp"], 
                                          "dm": container_name,
                                          "um": data['um']
                                          })
    except requests.exceptions.RequestException as e:
        print(f"Failed to contact logging_capstone: {e}")

    return jsonify({"status": "success"}), 200


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
    app.run(host='0.0.0.0', port=80)

from flask import Flask, request, jsonify
import requests
import threading
import time
import json
import os
import redis
from jaeger_client import Config
from flask_opentracing import FlaskTracing

def init_jaeger_tracer(service_name='your-service-name'):
    config = Config(
        config={
            'sampler': {'type': 'const', 'param': 1},
            'logging': True,
            'local_agent': {
                'reporting_host': os.environ.get('JAEGER_AGENT_HOST', "localhost"),
                'reporting_port': os.environ.get('JAEGER_AGENT_PORT', '6831'),
            },
        },
        service_name=service_name,
    )
    return config.initialize_tracer()



app = Flask(__name__)


container_name = os.environ.get("CONTAINER_NAME", "1")
redis_ip = os.environ.get("REDIS_IP_ADDRESS", "2")
redis_client = redis.StrictRedis(host=redis_ip, port=6379)
start_time = ''
jaeger_tracer = init_jaeger_tracer(container_name[:5])
tracing = FlaskTracing(jaeger_tracer, True, app)



def contact_containers(calls):
    # if not root: 
    # while True:
    #     if os.path.exists('start_time.txt'):
    #         with open('start_time.txt') as f:
    #             start_time = int(f.read().strip())
    #             break
    #     time.sleep(1)
    # else: 
    #     start_time = str(int(time.time()))
    calls_list = list(map(int, calls.keys()))
    calls_list.sort()
    while True:
        print("Waiting for start_time")
        if redis_client.exists('start_time'):
            print("start_time found")
            start_time = int(redis_client.get('start_time'))
            break
        time.sleep(1)

    while True:
        timestamp = int(time.time()) - start_time
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
                    print(f"sent request to {container}")
                    response = requests.post(f"http://{container[:5]}/", 
                                             json={"timestamp": timestamp, "um": container_name})
                    print(f"Contacted {container}: {response.text}")
                except requests.exceptions.RequestException as e:
                    print(f"Failed to contact {container[:5]}: {e}")
        time.sleep(1)

@app.route('/', methods=['POST'])
def home():
    data = request.get_json()
    # log
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
    if calls:
        threading.Thread(target=contact_containers, args=(calls,)).start()
    app.run(host='0.0.0.0', port=80)

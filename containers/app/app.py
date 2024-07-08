from flask import Flask, request
import requests
import threading
import time
import json

app = Flask(__name__)


def contact_containers(calls):
    while True:
        timestamp = str(int(time.time()))
        if timestamp in calls:
            for container in calls[timestamp]:
                try:
                    response = requests.post(f"http://{container}/", data=f"Hello from {request.host}")
                    print(f"Contacted {container}: {response.text}")
                except requests.exceptions.RequestException as e:
                    print(f"Failed to contact {container}: {e}")
        time.sleep(1)

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'GET':
        message = f"Received GET request on {request.path}"
        print(message)
        return message, 200
    elif request.method == 'POST':
        post_data = request.data.decode('utf-8')
        message = f"Received POST request on {request.path} with body: {post_data}"
        print(message)
        return message, 200

if __name__ == "__main__":
    # Load the contact data from a JSON file
    # with open('calls.json') as f:
    #     calls = json.load(f)
    # if calls:
    #     threading.Thread(target=contact_containers).start()
    app.run(host='0.0.0.0', port=80)

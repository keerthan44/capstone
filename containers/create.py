import docker
import os
import json
import redis
import time

# Initialize Docker client
client = docker.from_env()

# Function to check if the network exists
def get_or_create_network(network_name):
    try:
        network = client.networks.get(network_name)
        print(f"Network '{network_name}' already exists.")
    except docker.errors.NotFound:
        network = client.networks.create(network_name)
        print(f"Network '{network_name}' created.")
    return network

def create_json_file(container_name, data):
    # Generate JSON data specific to the container
    json_data = data
    
    print(data)
    # Create a temporary file with the JSON data
    with open(f"{container_name}.json", "w") as temp_file:
        json.dump(json_data, temp_file)
        temp_file_path = temp_file.name

    return temp_file_path

# Function to read container names from a file
def read_container_names(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

# Function to create and run a container
def create_container(container_name, network_name, data, ip_address):
    print(network_name)
    json_file_path = create_json_file(container_name, data)
    container = client.containers.run(
        "flask-contact-container",
        name=container_name,
        hostname=container_name[:5],
        detach=True,
        network=network_name,
        ports={'80/tcp': None},
        environment={"CONTAINER_NAME": container_name,
                     "REDIS_IP_ADDRESS": ip_address},
    )
    os.system(f"docker cp ./{container_name}.json {container.id}:/app/calls.json")
    os.remove(json_file_path)
    print(f"Container '{container_name}' created and started.")
    return container

def create_logging_container(network_name, redis_ip):
    container = client.containers.run(
        'logging_capstone',
        name="logging_capstone",
        hostname='logging_capstone',
        detach=True,
        network=network_name,
        ports={'80/tcp': None},
        environment={"REDIS_IP_ADDRESS": redis_ip}
    )
    print(f"Logging Container '{container.id}' created and started.")
    return container

def create_redis_container(network_name):
    container = client.containers.run(
        "redis:latest", 
        name="redis_capstone", 
        hostname="redis_capstone", 
        ports={'6379/tcp': None}, 
        detach=True,
        network=network_name
    )
    print(f"Redis container started with ID: {container.id}")
    return container
    # ip_add = container.attrs['NetworkSettings']["Networks"][network_name]["IPAddress"]


# Main function
def main():
    network_name = "static_application"
    container_names_file = "containers.txt"

    # Ensure the network exists
    network = get_or_create_network(network_name)

    # Read container names from file
    container_names = read_container_names(container_names_file)

    #Create Redis Container
    redis_container = create_redis_container(network_name)
    redis_container.reload()
    print(redis_container.attrs['NetworkSettings'])
    ip_address = redis_container.attrs['NetworkSettings']["Networks"][network_name]["IPAddress"]
    print("Redis container is up and running.")

    # Create Logging Container
    create_logging_container(network_name, ip_address)
    print("Logging container is up and running.")

    with open('calls.json') as f:
        calls = json.load(f)
    # # Create and run containers
    containers = [create_container(name, network_name, calls[name] if name in calls else {}, ip_address) for name in container_names]

    print("All containers are up and running.")

    redis_client = redis.StrictRedis(host=ip_address, port=6379)
    redis_client.set('start_time', int(time.time()))

    print("Redis Start Time value is now set at", redis_client.get('start_time'))
    print("Containers will start communicating")



if __name__ == "__main__":
    main()

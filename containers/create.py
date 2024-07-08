import docker
import os
import tempfile
import json

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
def create_container(container_name, network_name, data):
    json_file_path = create_json_file(container_name, data)
    container = client.containers.run(
        "flask-contact-container",
        name=container_name,
        detach=True,
        network=network_name,
        ports={'80/tcp': None},
        volumes={json_file_path: {'bind': '/app/calls.json', 'mode': 'ro'}}
    )
    os.remove(json_file_path)
    print(f"Container '{container_name}' created and started.")
    return container

# Main function
def main():
    network_name = "static_application"
    container_names_file = "containers.txt"

    # Ensure the network exists
    network = get_or_create_network(network_name)

    # Read container names from file
    container_names = read_container_names(container_names_file)

    with open('calls.json') as f:
        calls = json.load(f)
    # # Create and run containers
    containers = [create_container(name, network_name, calls[name] if name in calls else {}) for name in container_names]

    print("All containers are up and running.")

if __name__ == "__main__":
    main()

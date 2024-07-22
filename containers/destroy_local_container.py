import docker
from multiprocessing import Pool
import os
import json

def read_container_names(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def stop_and_remove_container(container_name):
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        print(f"Stopping container {container.name} ({container.id})...")
        container.stop()
        print(f"Removing container {container.name} ({container.id})...")
        container.remove()
    except docker.errors.NotFound:
        print(f"Container {container_name} does not exist")

def main():
    mappedContainersFile = "containers_mapped.json"
    container_names = []
    if os.path.isfile(mappedContainersFile):
        with open(mappedContainersFile) as f:
            container_names = json.load(f)
        container_names = container_names.values() 
    else:
        container_names = read_container_names('containers.txt')
    
    # Use Pool to manage multiple processes
    with Pool(processes=len(container_names)) as pool:
        pool.map(stop_and_remove_container, container_names)
    stop_and_remove_container('redis_capstone')
    stop_and_remove_container('logging_capstone')

if __name__ == "__main__":
    main()

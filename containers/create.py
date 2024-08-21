import os
import json
import time
import redis
from kubernetes import client, config
from kubernetes.client import V1Pod, V1Container, V1ObjectMeta, V1PodSpec, V1Service, V1ServiceSpec, V1ServicePort, V1Deployment, V1DeploymentSpec, V1PodTemplateSpec, V1LabelSelector, V1Ingress, V1IngressSpec, V1IngressRule, V1IngressBackend

from dotenv import load_dotenv
import subprocess

load_dotenv()  # take environment variables from .env.

# Load the Kubernetes configuration
config.load_kube_config()

# Kubernetes API client
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
networking_v1 = client.NetworkingV1Api()  # For Ingress

dockerUsername = os.getenv("DOCKER_USERNAME")

def wait_for_pods_ready(namespace):
    # Load kube config and create a client
    config.load_kube_config()
    v1 = client.CoreV1Api()

    while True:
        pods = v1.list_namespaced_pod(namespace)
        if all(pod.status.phase == 'Running' for pod in pods.items):
            print("All pods are running.")
            break
        print("Waiting for pods to be ready...")
        time.sleep(1)

def port_forward_and_update_time(namespace, service_name, local_port=6379, remote_port=6379):

    # Start port forwarding in a subprocess
    process = subprocess.Popen([
        'kubectl', 'port-forward', f'service/{service_name}', f'{local_port}:{remote_port}', '-n', namespace
    ])

    # Wait a few seconds to ensure port forwarding is established
    time.sleep(5)

    # Connect to Redis
    r = redis.Redis(host='localhost', port=local_port)
    print("Connected to Redis.")
    
    # Example operation: Get a value from Redis
    try:
        r.set('start_time', time.time_ns())  # Store time in miliseconds

        print("Redis Start Time value is now set at", r.get('start_time'))
        print("Containers will start communicating")
    except Exception as e:
        print(f"Error: {e}")
    
    # Optionally, you can stop the port forwarding process if needed
    process.terminate()

# Function to read container names from a file
def read_container_names(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def get_and_rename_containers(containersFile="containers.txt", callsFile="calls.json"):
    mappedContainersFile = "".join(containersFile.split(".")[0]) + "_mapped.json"
    
    # Check if the mapped containers file already exists
    if os.path.isfile(mappedContainersFile):
        with open(mappedContainersFile) as f:
            renamed_containers = json.load(f)
        
        with open(callsFile) as f:
            calls = json.load(f)
        
        for um in list(calls.keys()):
            for timestamp in calls[um]:
                for i, dm_entry in enumerate(calls[um][timestamp]):
                    dm_service = dm_entry['dm_service']
                    if dm_service not in renamed_containers:
                        print(f"Warning: {dm_service} not found in renamed_containers. Skipping.")
                        continue
                    # Rename the dm_service field
                    calls[um][timestamp][i]['dm_service'] = renamed_containers[dm_service]
            if um in renamed_containers:
                calls[renamed_containers[um]] = calls.pop(um)
            else:
                print(f"Warning: {um} not found in renamed_containers. Skipping.")
        
        # Write the renamed calls to a new mapped file
        with open(callsFile.split(".")[0] + "_mapped.json", "w") as f:
            json.dump(calls, f, indent=4)
        return renamed_containers.values(), calls
    
    # If mapped containers file doesn't exist, proceed with renaming
    containers = read_container_names(containersFile)
    renamed_containers = {container: f"s{i}" for i, container in enumerate(containers, 1)}
    
    with open(callsFile) as f:
        calls = json.load(f)
    
    for um in list(calls.keys()):
        for timestamp in calls[um]:
            for i, dm_entry in enumerate(calls[um][timestamp]):
                dm_service = dm_entry['dm_service']
                if dm_service not in renamed_containers:
                    print(f"Warning: {dm_service} not found in renamed_containers. Skipping.")
                    continue
                # Rename the dm_service field
                calls[um][timestamp][i]['dm_service'] = renamed_containers[dm_service]
        if um in renamed_containers:
            calls[renamed_containers[um]] = calls.pop(um)
        else:
            print(f"Warning: {um} not found in renamed_containers. Skipping.")
    
    # Write the renamed containers to a mapped file
    with open(mappedContainersFile, "w") as f:
        json.dump(renamed_containers, f, indent=4)
    
    # Write the renamed calls to a new mapped file
    with open(callsFile.split(".")[0] + "_mapped.json", "w") as f:
        json.dump(calls, f, indent=4)
    
    return renamed_containers.values(), calls

def addContainerJob(container_names):
    while True:
        print("Menu: ")
        print("1. All Containers are sleeping")
        print("2. No Containers are sleeping")
        print("3. Some Containers are sleeping")
        option = input("Enter your choice (1/2/3): ").strip()
        match option:
            case '1': 
                return [[container_name, 0] for container_name in container_names]
            case '2': 
                return [[container_name, 1] for container_name in container_names]
            case '3':
                print("You can enter the containers that are working in ranges like 1-5, 7-10")
                print("1-1(includes both 1 and 1), 1-4(includes 1, 2, 3, 4)")
                containersWorking = input("Enter the containers that are working: ").replace(" ", "").split(",")
                print(containersWorking)
                for i in range(len(containersWorking)):
                    start, end = containersWorking[i].split("-")
                    containersWorking[i] = [int(start), int(end)]
                n = len(container_names) 
                container_names = [[container_name, 0] for container_name in container_names]
                for start, end in containersWorking:
                    for index in range(start, end + 1):
                        if index < n:
                            container_names[index][1] = 1
                return container_names
            case _:
                print("Invalid choice. Please try again.")

def get_or_create_namespace(namespace_name):
    try:
        v1.read_namespace(name=namespace_name)
        print(f"Namespace '{namespace_name}' already exists.")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            v1.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace_name)))
            print(f"Namespace '{namespace_name}' created.")
        else:
            raise

def create_config_map(namespace, config_name, data):
    config_map = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(name=config_name, namespace=namespace),
        data={"data": data}
    )
    print(v1.create_namespaced_config_map(namespace=namespace, body=config_map))
    print(f"ConfigMap '{config_name}' created in namespace '{namespace}'.")

def create_redis_service(namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="redis-service", namespace=namespace),
        spec=V1ServiceSpec(
            selector={"app": "redis"},
            ports=[V1ServicePort(port=6379, target_port=6379)]
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Redis Service created in namespace '{namespace}'.")

def create_redis_deployment(namespace):
    container = V1Container(
        name="redis",
        image="redis:latest",
        ports=[client.V1ContainerPort(container_port=6379)]
    )
    pod_spec = V1PodSpec(containers=[container])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": "redis"}), spec=pod_spec)
    spec = V1DeploymentSpec(replicas=1, template=template, selector=V1LabelSelector(match_labels={"app": "redis"}))
    deployment = V1Deployment(metadata=V1ObjectMeta(name="redis-deployment", namespace=namespace), spec=spec)
    
    apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
    print(f"Redis Deployment created in namespace '{namespace}'.")

def create_logging_service(namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="logging-service", namespace=namespace),
        spec=V1ServiceSpec(
            selector={"app": "logging"},
            ports=[V1ServicePort(port=80, target_port=80)]
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Logging Service created in namespace '{namespace}'.")

def create_logging_deployment(namespace, redis_ip):
    container = V1Container(
        name="logging-container",
        image=f"logging_capstone",
        env=[client.V1EnvVar(name="REDIS_IP_ADDRESS", value=redis_ip)],
        ports=[client.V1ContainerPort(container_port=80)],
        image_pull_policy="Never"  # Set the image pull policy to Never
    )
    pod_spec = V1PodSpec(containers=[container])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": "logging"}), spec=pod_spec)
    spec = V1DeploymentSpec(replicas=1, template=template, selector=V1LabelSelector(match_labels={"app": "logging"}))
    deployment = V1Deployment(metadata=V1ObjectMeta(name="logging-deployment", namespace=namespace), spec=spec)
    
    apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
    print(f"Logging Deployment created in namespace '{namespace}'.")

def create_container_deployment(namespace, container_name, config_map_name, redis_ip, container_job):
    container = V1Container(
        name=container_name,
        image=f"flask-contact-container",
        env=[
            client.V1EnvVar(name="CONTAINER_NAME", value=container_name),
            client.V1EnvVar(name="REDIS_IP_ADDRESS", value=redis_ip),
            client.V1EnvVar(name="CONTAINER_JOB", value=str(container_job)),
            client.V1EnvVar(name="NAMESPACE", value=namespace)
        ],
        volume_mounts=[client.V1VolumeMount(mount_path="/app/calls.json", sub_path="data", name="config-volume")],
        image_pull_policy="Never"  # Set the image pull policy to Never
    )
    
    volume = client.V1Volume(
        name="config-volume",
        config_map=client.V1ConfigMapVolumeSource(name=config_map_name)
    )

    pod_spec = V1PodSpec(containers=[container], volumes=[volume])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": container_name}), spec=pod_spec)
    spec = V1DeploymentSpec(replicas=1, template=template, selector=V1LabelSelector(match_labels={"app": container_name}))
    deployment = V1Deployment(metadata=V1ObjectMeta(name=f"{container_name}-deployment", namespace=namespace), spec=spec)
    
    apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
    print(f"Deployment '{container_name}' created in namespace '{namespace}'.")

def create_container_service(namespace, container_name, port_mappings):
    """
    Create a Kubernetes service with multiple port mappings.
    
    :param namespace: Namespace where the service will be created
    :param container_name: Name of the container (used for naming the service and matching labels)
    :param port_mappings: List of tuples where each tuple contains (port, target_port)
    """
    # Create a list of V1ServicePort objects from the port mappings
    service_ports = [
        client.V1ServicePort(
            port=port_mapping['port'],       # Port that the service will expose
            target_port=port_mapping['target_port']  # Port on the container to forward traffic to
        ) for port_mapping in port_mappings
    ]
    
    # Create a service object
    service = client.V1Service(
        metadata=client.V1ObjectMeta(name=f"{container_name}-service", namespace=namespace),
        spec=client.V1ServiceSpec(
            selector={"app": container_name},  # Must match labels of the deployment pods
            ports=service_ports
        )
    )
    
    # Create the service in the specified namespace
    v1 = client.CoreV1Api()
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Service '{container_name}-service' created in namespace '{namespace}' with ports: {port_mappings}.")


def main():
    namespace = os.getenv("KUBERNETES_NAMESPACE", "static-application")
    container_names_file = "containers.txt"
    
    # Ensure the namespace exists
    get_or_create_namespace(namespace)

    # Read container names from file and get renamed containers
    container_names, calls = get_and_rename_containers()

    # Create Redis deployment and service
    create_redis_deployment(namespace)
    create_redis_service(namespace)
    wait_for_pods_ready(namespace)
    
    # Create Logging deployment and service
    create_logging_deployment(namespace, redis_ip="redis-service")
    create_logging_service(namespace)

    # Create ConfigMap for each container's calls.json
    for container_name in container_names:
        create_config_map(namespace, f"{container_name}-config", data=json.dumps(calls.get(container_name, {})))

    # Create deployments for containers
    container_jobs = addContainerJob(container_names)
    for container_name, container_job in container_jobs:
        create_container_deployment(namespace, container_name, f"{container_name}-config", redis_ip="redis-service", container_job=container_job)
    for container_name in container_names:
        create_container_service(namespace, container_name, [{ "port": 80, "target_port": 80 }])

    print("All deployments and services are up and running in Kubernetes.")
    wait_for_pods_ready(namespace)
    port_forward_and_update_time(namespace, "redis-service", 60892, 6379)


if __name__ == "__main__":
    main()

import os
import json
from kubernetes import client, config
from kubernetes.client import V1Pod, V1Container, V1ObjectMeta, V1PodSpec, V1Service, V1ServiceSpec, V1ServicePort, V1Deployment, V1DeploymentSpec, V1PodTemplateSpec, V1LabelSelector, V1Ingress, V1IngressSpec, V1IngressRule, V1IngressBackend
from kafka_setup import create_kafka_topic, deploy_kafka_environment
from utils import wait_for_pods_ready, port_forward_and_exec_func, wait_for_service_ready, get_or_create_namespace   

from dotenv import load_dotenv
from redis_setup import set_start_time_redis, create_redis_deployment, create_redis_service

load_dotenv()  # take environment variables from .env.


dockerUsername = os.getenv("DOCKER_USERNAME")



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

def create_config_map(v1, namespace, config_name, data):
    config_map = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(name=config_name, namespace=namespace),
        data={"data": data}
    )
    v1.create_namespaced_config_map(namespace=namespace, body=config_map)
    print(f"ConfigMap '{config_name}' created in namespace '{namespace}'.")



def create_logging_service(v1, namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="logging-service", namespace=namespace),
        spec=V1ServiceSpec(
            selector={"app": "logging"},
            ports=[V1ServicePort(port=80, target_port=80)]
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Logging Service created in namespace '{namespace}'.")

def create_logging_deployment(apps_v1, namespace, redis_ip):
    container = V1Container(
        name="logging-container",
        image=f"logging_capstone",
        env=[client.V1EnvVar(name="REDIS_IP_ADDRESS", value=redis_ip)],
        ports=[client.V1ContainerPort(container_port=80)],
        image_pull_policy="IfNotPresent"  # Set the image pull policy to IfNotPresent
    )
    pod_spec = V1PodSpec(containers=[container])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": "logging"}), spec=pod_spec)
    spec = V1DeploymentSpec(replicas=1, template=template, selector=V1LabelSelector(match_labels={"app": "logging"}))
    deployment = V1Deployment(metadata=V1ObjectMeta(name="logging-deployment", namespace=namespace), spec=spec)
    
    apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
    print(f"Logging Deployment created in namespace '{namespace}'.")

def create_container_deployment(apps_v1, namespace, container_name, config_map_name, redis_ip, container_job):
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
        image_pull_policy="IfNotPresent"  # Set the image pull policy to IfNotPresent
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

def create_container_service(v1, namespace, container_name, port_mappings):
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
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Service '{container_name}-service' created in namespace '{namespace}' with ports: {port_mappings}.")

def main():
    namespace = os.getenv("KUBERNETES_NAMESPACE", "static-application")
    container_names_file = "containers.txt"
    # Load the Kubernetes configuration
    config.load_kube_config()

    # Kubernetes API client
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    rbac_v1 = client.RbacAuthorizationV1Api()

    
    # # Ensure the namespace exists
    get_or_create_namespace(namespace)

    # Setup Kafka environment
    deploy_kafka_environment(v1, apps_v1, rbac_v1, namespace)

    # Read container names from file and get renamed containers
    # container_names, calls = get_and_rename_containers()

    # # Create Redis deployment and service
    # create_redis_deployment(namespace)
    # create_redis_service(namespace)
    # wait_for_pods_ready(namespace)

    # port_forward_and_exec_func(namespace, "kafka-service", 9092, 30092, funcToExec=create_kafka_topic, data={"topic_names": container_names, 'namespace': namespace})
    # create_kafka_topic({"topic_names": container_names, "local_port": 30992, 'namespace': namespace})
    
    # # Create Logging deployment and service
    # create_logging_deployment(namespace, redis_ip="redis-service")
    # create_logging_service(namespace)

    # # Create ConfigMap for each container's calls.json
    # for container_name in container_names:
    #     create_config_map(namespace, f"{container_name}-config", data=json.dumps(calls.get(container_name, {})))

    # # Create deployments for containers
    # container_jobs = addContainerJob(container_names)
    # for container_name, container_job in container_jobs:
    #     create_container_deployment(namespace, container_name, f"{container_name}-config", redis_ip="redis-service", container_job=container_job)
    # for container_name in container_names:
    #     create_container_service(namespace, container_name, [{ "port": 80, "target_port": 80 }])

    # print("All deployments and services are up and running in Kubernetes.")
    # wait_for_pods_ready(namespace)
    # port_forward_and_exec_func(namespace, "redis-service", 60892, 6379, funcToExec=set_start_time_redis)


if __name__ == "__main__":
    main()

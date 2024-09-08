import os
import json
import random
from collections import defaultdict
from kubernetes import client, config
from kubernetes.client import V1Deployment, V1DeploymentSpec, V1Container, V1ObjectMeta, V1PodSpec, V1Service, V1ServiceSpec, V1ServicePort, V1StatefulSet, V1StatefulSetSpec, V1PodTemplateSpec, V1LabelSelector, V1Volume, V1ConfigMapVolumeSource, V1VolumeMount
from kafka_setup import deploy_kafka_environment, create_topics_http_request
from utils import wait_for_pods_ready, port_forward_and_exec_func, get_or_create_namespace   
from dotenv import load_dotenv
from redis_setup import deploy_redis_environment, set_start_time_redis

load_dotenv()  # take environment variables from .env.

dockerUsername = os.getenv("DOCKER_USERNAME")

def read_container_names(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def get_and_rename_containers(containersFile="containers.json", callsFile="calls.json"):
    mappedContainersFile = containersFile.split(".")[0] + "_mapped.json"
    
    containers = read_container_names(containersFile)
    
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
                    calls[um][timestamp][i]['dm_service'] = renamed_containers[dm_service]["mappedName"]
            if um in renamed_containers:
                calls[renamed_containers[um]['mappedName']] = calls.pop(um)
            else:
                print(f"Warning: {um} not found in renamed_containers. Skipping.")
        
        with open(callsFile.split(".")[0] + "_mapped.json", "w") as f:
            json.dump(calls, f, indent=4)
        return renamed_containers, calls
    
    renamed_containers = {container['msName']: {"mappedName" : f"s{i}", "containerIndex": i, **container} for i, container in enumerate(containers, 1)}
    
    with open(callsFile) as f:
        calls = json.load(f)
    
    for um in list(calls.keys()):
        for timestamp in calls[um]:
            for i, dm_entry in enumerate(calls[um][timestamp]):
                dm_service = dm_entry['dm_service']
                if dm_service not in renamed_containers:
                    print(f"Warning: {dm_service} not found in renamed_containers. Skipping.")
                    continue
                calls[um][timestamp][i]['dm_service'] = renamed_containers[dm_service]["mappedName"]
        if um in renamed_containers:
            calls[renamed_containers[um]['mappedName']] = calls.pop(um)
        else:
            print(f"Warning: {um} not found in renamed_containers. Skipping.")
    
    with open(callsFile.split(".")[0] + "_mapped.json", "w") as f:
        json.dump(calls, f, indent=4)
    
    with open(mappedContainersFile, 'w') as f:
        json.dump(renamed_containers, f, indent=4)

    return renamed_containers, calls

def addContainerJob(containers):
    while True:
        print("Menu: ")
        print("1. All Containers are sleeping")
        print("2. No Containers are sleeping")
        print("3. Some Containers are sleeping")
        option = input("Enter your choice (1/2/3): ").strip()
        match option:
            case '1': 
                return {container: {**containers[container], "containerJob": 0} for container in containers}
            case '2': 
                return {container: {**containers[container], "containerJob": 1} for container in containers}
            case '3':
                print("You can enter the containers that are working in ranges like 1-5, 7-10")
                print("1-1(includes both 1 and 1), 1-4(includes 1, 2, 3, 4)")
                containersWorking = input("Enter the containers that are working: ").replace(" ", "").split(",")
                print(containersWorking)
                for i in range(len(containersWorking)):
                    start, end = containersWorking[i].split("-")
                    containersWorking[i] = [int(start), int(end)]
                for container in containers:
                    addedJob = False
                    for workingRange in containersWorking:
                        if workingRange[0] <= containers[container]['containerIndex'] <= workingRange[1]:
                            addedJob = True
                            containers[container]["containerJob"] = 1
                            break
                    if not addedJob:
                        containers[container]["containerJob"] = 0
                return containers 
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
        image_pull_policy="IfNotPresent"
    )
    pod_spec = V1PodSpec(containers=[container])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": "logging"}), spec=pod_spec)
    spec = V1DeploymentSpec(replicas=1, template=template, selector=V1LabelSelector(match_labels={"app": "logging"}))
    deployment = V1Deployment(metadata=V1ObjectMeta(name="logging-deployment", namespace=namespace), spec=spec)
    
    apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
    print(f"Logging Deployment created in namespace '{namespace}'.")

def create_container_statefulset(apps_v1, namespace, container_name, config_map_name, kafka_replicas, redis_ip, container_job, replicas=1):
    container = V1Container(
        name=container_name,
        image=f"flask-contact-container",
        env=[
            client.V1EnvVar(name="CONTAINER_NAME", value=container_name),
            client.V1EnvVar(name="REDIS_IP_ADDRESS", value=redis_ip),
            client.V1EnvVar(name="CONTAINER_JOB", value=str(container_job)),
            client.V1EnvVar(name="NAMESPACE", value=namespace),
            client.V1EnvVar(name="KAFKA_REPLICAS", value=str(kafka_replicas)),
            client.V1EnvVar(name="POD_NAME", value_from=client.V1EnvVarSource(field_ref=client.V1ObjectFieldSelector(field_path="metadata.name"))),

        ],
        volume_mounts=[client.V1VolumeMount(mount_path="/app/calls.json", sub_path="data", name="config-volume")],
        image_pull_policy="IfNotPresent"
    )
    
    volume = client.V1Volume(
        name="config-volume",
        config_map=client.V1ConfigMapVolumeSource(name=config_map_name)
    )

    pod_spec = V1PodSpec(containers=[container], volumes=[volume])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": container_name}), spec=pod_spec)

    stateful_set_spec = V1StatefulSetSpec(
        service_name=f"{container_name}-service",
        replicas=replicas,
        selector=V1LabelSelector(match_labels={"app": container_name}),
        template=template,
        volume_claim_templates=[client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name=f"{container_name}-data"),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=client.V1ResourceRequirements(
                    requests={"storage": "1Gi"}
                )
            )
        )],
        pod_management_policy="OrderedReady",
        update_strategy=client.V1StatefulSetUpdateStrategy(type="RollingUpdate")
    )

    stateful_set = V1StatefulSet(
        metadata=V1ObjectMeta(name=f"{container_name}-statefulset", namespace=namespace),
        spec=stateful_set_spec
    )
    
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=stateful_set)
    print(f"StatefulSet '{container_name}-statefulset' created in namespace '{namespace}' with {replicas} replicas.")

def create_container_service(v1, namespace, container_name, port_mappings):
    service_ports = [
        client.V1ServicePort(
            port=port_mapping['port'],
            target_port=port_mapping['target_port'],
            name=port_mapping['name']
        ) for port_mapping in port_mappings
    ]
    
    service = client.V1Service(
        metadata=client.V1ObjectMeta(name=f"{container_name}-service", namespace=namespace),
        spec=client.V1ServiceSpec(
            selector={"app": container_name},
            ports=service_ports
        )
    )
    
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Service '{container_name}-service' created in namespace '{namespace}' with ports: {port_mappings}.")

# def split_calls_to_replicas(data, replicas, mappedName, choice):
#     # print("Data: ",data)
#     # print("replicas: ",replicas)
#     # print("mappedName: ",mappedName)
#     return {f"{mappedName}-statefulset-{str(i)}": data for i in range(replicas)}

def split_calls_to_replicas(data, replicas, mappedName, choice):
    # Initialize the result dictionary
    result = {f"{mappedName}-statefulset-{i}": defaultdict(list) for i in range(replicas)}

    # Convert data into a list of calls, each with a timestamp
    calls = [(t, call) for t, calls_list in data.items() for call in calls_list]
    
    if choice == "round_robin":
        # Distribute calls in a round-robin fashion
        for idx, (timestamp, call) in enumerate(calls):
            statefulset_index = idx % replicas
            result[f"{mappedName}-statefulset-{statefulset_index}"][timestamp].append(call)
    
    elif choice == "random":
        # Distribute calls randomly
        for timestamp, call in calls:
            statefulset_index = random.randint(0, replicas - 1)
            result[f"{mappedName}-statefulset-{statefulset_index}"][timestamp].append(call)
    
    else:
        raise ValueError("Invalid choice. Please select 'random' or 'round_robin'.")
    
    return result

def main():
    NAMESPACE = os.getenv("KUBERNETES_NAMESPACE", "static-application")
    KAFKA_EXTERNAL_GATEWAY_NODEPORT = int(os.getenv("KAFKA_EXTERNAL_GATEWAY_NODEPORT", "32092"))
    container_names_file = "containers_fake.txt"
    config.load_kube_config()

    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    rbac_v1 = client.RbacAuthorizationV1Api()

    get_or_create_namespace(NAMESPACE)

    (kafka_replicas, kafka_statefulset_name, kafka_headless_service_name, kakfa_gateway_service_name) = deploy_kafka_environment(NAMESPACE, v1, apps_v1, rbac_v1, KAFKA_EXTERNAL_GATEWAY_NODEPORT)

    renamed_containers, calls = get_and_rename_containers()

    (redis_service_name, ) = deploy_redis_environment(NAMESPACE, v1, apps_v1)
    wait_for_pods_ready(NAMESPACE)

    create_logging_deployment(apps_v1, NAMESPACE, redis_ip=redis_service_name)
    create_logging_service(v1, NAMESPACE)
    
    choice = input("Do you want random assignment of calls between instance IDs or round robin assignment?\n (Enter 'random' or 'round_robin'): ").strip().lower()

    topics = []
    for container in renamed_containers:
        containerKeys = renamed_containers[container]
        mappedName = containerKeys['mappedName']
        replicas = containerKeys['replicas']
        topics.append({ "name": mappedName, "partitions": 1, "replication_factor": kafka_replicas })
        
        config_map_name = f"{mappedName}-config"
        data = split_calls_to_replicas(calls.get(mappedName, {}), replicas, mappedName, choice)
        # print(data)
        create_config_map(v1, NAMESPACE, config_map_name, data=json.dumps(data))
    
    create_topics_http_request(topics, NAMESPACE, kafka_statefulset_name, kakfa_gateway_service_name, kafka_headless_service_name, KAFKA_EXTERNAL_GATEWAY_NODEPORT)

    renamed_containers = addContainerJob(renamed_containers)
    for container_name in renamed_containers:
        containerKeys = renamed_containers[container_name]
        mappedName = containerKeys['mappedName']
        containerJob = containerKeys['containerJob']
        replicas = containerKeys['replicas']
        
        # Create services for each container
        create_container_service(v1, NAMESPACE, mappedName, [{ "port": 80, "target_port": 80, 'name': 'flask-service' }, { "port": 50051, "target_port": 50051, "name": 'grpc-service' }])
        
        config_map_name = f"{mappedName}-config"
        create_container_statefulset(apps_v1, NAMESPACE, mappedName, config_map_name, kafka_replicas, redis_ip=redis_service_name, container_job=containerJob, replicas=replicas)

    wait_for_pods_ready(NAMESPACE)
    print("All statefulsets, deployments and services are up in Kubernetes.")
    port_forward_and_exec_func(NAMESPACE, redis_service_name, 60892, 6379, funcToExec=set_start_time_redis)

if __name__ == "__main__":
    main()

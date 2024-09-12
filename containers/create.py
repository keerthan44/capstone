import math
import os
import json
import random
from collections import defaultdict
from kubernetes import client, config
from kubernetes.client import V1EnvVar, V1EnvVarSource, V1ObjectFieldSelector, V1PersistentVolumeClaimVolumeSource, V1Container, V1ObjectMeta, V1PodSpec, V1Service, V1ServiceSpec, V1ServicePort, V1StatefulSet, V1StatefulSetSpec, V1PodTemplateSpec, V1LabelSelector, V1Volume, V1ConfigMapVolumeSource, V1VolumeMount
from kafka_setup import deploy_kafka_environment, create_topics_http_request
from utils import wait_for_pods_ready, port_forward_and_exec_func, get_or_create_namespace, wait_for_all_jobs_to_complete, delete_completed_jobs, wait_for_job_completion, get_docker_image_with_pre_suffix, delete_all_configmaps
from dotenv import load_dotenv
from redis_setup import deploy_redis_environment, set_start_time_redis

load_dotenv()  # take environment variables from .env.

dockerUsername = os.getenv("DOCKER_USERNAME")
MAX_K8S_API_LIMIT = 512 * 1024  # 3MB limit in bytes

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

def create_logging_statefulset(apps_v1, namespace, redis_ip):
    container = V1Container(
        name="logging-container",
        image=get_docker_image_with_pre_suffix("logging_capstone"),
        env=[client.V1EnvVar(name="REDIS_IP_ADDRESS", value=redis_ip)],
        ports=[client.V1ContainerPort(container_port=80)],
        image_pull_policy="IfNotPresent"
    )
    
    volume = client.V1Volume(
        name="logging-config-volume",
        config_map=client.V1ConfigMapVolumeSource(name="logging-config")
    )

    pod_spec = V1PodSpec(containers=[container], volumes=[volume])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": "logging"}), spec=pod_spec)
    
    stateful_set_spec = V1StatefulSetSpec(
        service_name="logging-service",
        replicas=1,
        selector=V1LabelSelector(match_labels={"app": "logging"}),
        template=template,
        volume_claim_templates=[client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name="logging-data"),
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
        metadata=V1ObjectMeta(name="logging-statefulset", namespace=namespace),
        spec=stateful_set_spec
    )
    
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=stateful_set)
    print(f"Logging StatefulSet created in namespace '{namespace}'.")

def calculate_storage_size(data_str):
    # Calculate the size of the JSON data in bytes
    data_length = len(data_str)
    
    # Convert bytes to MiB (1 MiB = 1024 * 1024 bytes)
    size_in_mib = math.ceil(data_length / (1024 * 1024))
    
    # Convert to GiB if needed (optional, but usually MiB is fine for most uses)
    # size_in_gib = math.ceil(size_in_mib / 1024)
    
    # Choose to use GiB or MiB based on your requirements
    # For simplicity, we'll use MiB here
    return f"{size_in_mib + 100}Mi"

def create_pvc(v1, namespace, pvc_name, data_str):
    size = calculate_storage_size(data_str)

    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=pvc_name, namespace=namespace),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=['ReadOnlyMany'],
            resources=client.V1ResourceRequirements(
                requests={'storage': size}
            )
        )
    )

    v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
    print(f"PersistentVolumeClaim '{pvc_name}' created in namespace '{namespace}'.")

def split_data(data_str, chunk_size=MAX_K8S_API_LIMIT):
    """
    Splits the data_str into chunks of chunk_size (3MB by default).
    """
    return [data_str[i:i + chunk_size] for i in range(0, len(data_str), chunk_size)]

def chunk_terminator_next(index):
    """
    Add a unique terminator to the end of each chunk to ensure that the chunks are processed in order.
    """
    return f"&&__NEXT_CHUNK_{index}__&&"

def create_job_with_chunk(batch_v1, namespace, job_name, pvc_name, chunk, chunk_index, isLastChunk, check_value=None):
    """
    Create a single Kubernetes Job to write a chunk of data to the PVC.
    Continuously check the file's last N characters, where N is the length of `check_value`,
    waiting until `check_value` is present before deleting those characters and writing the chunk.
    """
    chunk_job_name = f"{job_name}-{chunk_index}"

    # Get the length of the check_value
    check_value_length = len(check_value) + 1 if check_value else 0

    # Create a script with the necessary commands
    script_commands = []
    if check_value:
        script_commands.append(f'while [ "$(tail -c {check_value_length} /data/calls.json)" != "{check_value}" ]; do sleep 1; done;')
        script_commands.append(r"sed -i '$ s/.\{" + str(check_value_length - 1) + r"\}$//' /data/calls.json")
    
    if not isLastChunk:
        chunk += chunk_terminator_next(chunk_index + 1)
    
    script_commands.append(f'echo "{chunk}" >> /data/calls.json')

    script_content = '\n'.join(script_commands)

    # Create the ConfigMap with the script
    config_map = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(name=f'{chunk_job_name}-script', namespace=namespace),
        data={'script.sh': script_content}
    )

    # Apply the ConfigMap
    core_v1 = client.CoreV1Api()
    core_v1.create_namespaced_config_map(namespace=namespace, body=config_map)

    # Create the job definition
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=chunk_job_name, namespace=namespace),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name='populate-container',
                            image='busybox',
                            command=['sh', '/scripts/script.sh'],
                            volume_mounts=[
                                client.V1VolumeMount(
                                    mount_path='/scripts',
                                    name='script-volume'
                                ),
                                client.V1VolumeMount(
                                    mount_path='/data',
                                    name='my-pvc'
                                )
                            ]
                        )
                    ],
                    restart_policy='Never',
                    volumes=[
                        client.V1Volume(
                            name='script-volume',
                            config_map=client.V1ConfigMapVolumeSource(
                                name=f'{chunk_job_name}-script'
                            )
                        ),
                        client.V1Volume(
                            name='my-pvc',
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=pvc_name
                            )
                        )
                    ]
                )
            ),
            backoff_limit=0
        )
    )

    # Create the job in Kubernetes
    batch_v1.create_namespaced_job(namespace=namespace, body=job)
    print(f"Job '{chunk_job_name}' created in namespace '{namespace}' to write chunk {chunk_index} after check_value is found and deleted in PVC '{pvc_name}'.")



def create_jobs_with_data(batch_v1, namespace, job_name, pvc_name, data_str):
    """
    Split data_str into chunks and create multiple jobs, each handling one chunk.
    """
    # Split the data into chunks within the Kubernetes API limit
    data_chunks = split_data(data_str)
    chunks_numbers = len(data_chunks)
    create_job_with_chunk(batch_v1, namespace, job_name, pvc_name, data_chunks[0], 0, 1 == chunks_numbers)

    # Create a job for each chunk and wait for its completion before proceeding to the next
    if chunks_numbers > 1:
        for chunk_index, chunk in enumerate(data_chunks[1:], 1):
            create_job_with_chunk(batch_v1, namespace, job_name, pvc_name, chunk, chunk_index, chunk_index + 1 == chunks_numbers, check_value=chunk_terminator_next(chunk_index))

    print(f"Total {chunks_numbers} jobs created to handle the data.")


def create_container_statefulset(apps_v1, namespace, container_name, pvc_name, kafka_replicas, redis_ip, container_job, replicas=1):
    container = V1Container(
        name=container_name,
        image=get_docker_image_with_pre_suffix("flask-contact-container"),
        env=[
            V1EnvVar(name="CONTAINER_NAME", value=container_name),
            V1EnvVar(name="REDIS_IP_ADDRESS", value=redis_ip),
            V1EnvVar(name="CONTAINER_JOB", value=str(container_job)),
            V1EnvVar(name="NAMESPACE", value=namespace),
            V1EnvVar(name="KAFKA_REPLICAS", value=str(kafka_replicas)),
            V1EnvVar(name="POD_NAME", value_from=V1EnvVarSource(field_ref=V1ObjectFieldSelector(field_path="metadata.name"))),
        ],
        volume_mounts=[V1VolumeMount(mount_path="/app/data", name="data-volume")],
        image_pull_policy="Always"
    )

    volume = V1Volume(
        name="data-volume",
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name)
    )

    pod_spec = V1PodSpec(containers=[container], volumes=[volume])
    template = V1PodTemplateSpec(metadata=V1ObjectMeta(labels={"app": container_name}), spec=pod_spec)

    stateful_set_spec = V1StatefulSetSpec(
        service_name=f"{container_name}-service",
        replicas=replicas,
        selector=V1LabelSelector(match_labels={"app": container_name}),
        template=template,
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

def split_calls_to_replicas(data, replicas, mappedName, choice):
    # Initialize the result dictionary
    result = {f"{mappedName}-statefulset-{i}": defaultdict(list) for i in range(replicas)}

    # Convert data into a list of calls, each with a timestamp
    calls = [(t, call) for t, calls_list in data.items() for call in calls_list]
    
    if choice == "1":
        # Distribute calls in a round-robin fashion
        for idx, (timestamp, call) in enumerate(calls):
            statefulset_index = idx % replicas
            result[f"{mappedName}-statefulset-{statefulset_index}"][timestamp].append(call)
    
    elif choice == "0":
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
    batch_v1 = client.BatchV1Api()

    get_or_create_namespace(NAMESPACE)

    (kafka_replicas, kafka_statefulset_name, kafka_headless_service_name, kakfa_gateway_service_name) = deploy_kafka_environment(NAMESPACE, v1, apps_v1, rbac_v1, KAFKA_EXTERNAL_GATEWAY_NODEPORT)

    renamed_containers, calls = get_and_rename_containers()

    (redis_service_name, ) = deploy_redis_environment(NAMESPACE, v1, apps_v1)
    wait_for_pods_ready(NAMESPACE)

    create_logging_statefulset(apps_v1, NAMESPACE, redis_ip=redis_service_name)
    create_logging_service(v1, NAMESPACE)
    
    choice = input("Do you want random assignment of calls between instance IDs or round robin assignment?\n (Enter 0 for 'random' or 1 for 'round_robin'): ").strip().lower()

    topics = []
    for container in renamed_containers:
        containerKeys = renamed_containers[container]
        mappedName = containerKeys['mappedName']
        replicas = containerKeys['replicas']
        topics.append({ "name": mappedName, "partitions": 1, "replication_factor": kafka_replicas })
        
        pvc_name = f"{mappedName}-pvc"
        job_name = f"{mappedName}-job"
        data = split_calls_to_replicas(calls.get(mappedName, {}), replicas, mappedName, choice)
        # print(data)
        # create_config_map(v1, NAMESPACE, config_map_name, data=json.dumps(data))
        data_str = json.dumps(data).replace('"', '\\"')
        create_pvc(v1, NAMESPACE, pvc_name, data_str)
        create_jobs_with_data(batch_v1, NAMESPACE, job_name, pvc_name, data_str)
    wait_for_all_jobs_to_complete(batch_v1, NAMESPACE)
    delete_all_configmaps(v1, NAMESPACE)
    delete_completed_jobs(batch_v1, v1, NAMESPACE)
    
    create_topics_http_request(topics, NAMESPACE, kafka_statefulset_name, kakfa_gateway_service_name, kafka_headless_service_name, KAFKA_EXTERNAL_GATEWAY_NODEPORT)

    renamed_containers = addContainerJob(renamed_containers)
    for container_name in renamed_containers:
        containerKeys = renamed_containers[container_name]
        mappedName = containerKeys['mappedName']
        containerJob = containerKeys['containerJob']
        replicas = containerKeys['replicas']
        
        # Create services for each container
        create_container_service(v1, NAMESPACE, mappedName, [{ "port": 80, "target_port": 80, 'name': 'flask-service' }, { "port": 50051, "target_port": 50051, "name": 'grpc-service' }])
        
        pvc_name = f"{mappedName}-pvc"
        create_container_statefulset(apps_v1, NAMESPACE, mappedName, pvc_name, kafka_replicas, redis_ip=redis_service_name, container_job=containerJob, replicas=replicas)

    wait_for_pods_ready(NAMESPACE)
    print("All statefulsets, deployments and services are up in Kubernetes.")
    port_forward_and_exec_func(NAMESPACE, redis_service_name, 60892, 6379, funcToExec=set_start_time_redis)

if __name__ == "__main__":
    main()

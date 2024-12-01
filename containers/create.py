import math
import os
import json
import random
from collections import defaultdict
from kubernetes import client, config
from kubernetes.client import V1EnvVar, V1EnvVarSource, V1ObjectFieldSelector, V1PersistentVolumeClaimVolumeSource, V1Container, V1ObjectMeta, V1PodSpec, V1Service, V1ServiceSpec, V1ServicePort, V1StatefulSet, V1StatefulSetSpec, V1PodTemplateSpec, V1LabelSelector, V1Volume, V1ConfigMapVolumeSource, V1VolumeMount, V1SecurityContext, V1Lifecycle, V1LifecycleHandler, V1ExecAction
from kafka_setup import deploy_kafka_environment, create_topics_http_request
from utils import wait_for_pods_ready, port_forward_and_exec_func, get_or_create_namespace, wait_for_all_jobs_to_complete, delete_completed_jobs, wait_for_job_completion, get_docker_image_with_pre_suffix, delete_all_configmaps
from dotenv import load_dotenv
from redis_setup import deploy_redis_environment, set_start_time_redis
import psycopg2
import random
import time


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

def extract_remove_memcached_db_containers(renamed_containers, calls):
    db_values = {}
    memcached_values = {}

    # Create a reverse mapping from mapped names (e.g., s1, s2) to original names (e.g., USER, MS_41019)
    reverse_mapping = {details['mappedName']: original_name for original_name, details in renamed_containers.items()}

    # Iterate through the calls to filter based on the communication type
    for um in calls:
        for timestamp, dm_entries in calls[um].items():
            for entry in dm_entries:
                comm_type = entry.get('communication_type', '').strip().lower()  # Normalize the comm_type to lowercase and remove extra spaces
                dm_service = entry['dm_service']  # This will be something like s2, s3, etc.

                # Debug: print communication type and service to ensure values are correct
                #print(f"Processing service: {dm_service}, communication_type: {comm_type}")

                # Reverse lookup to find the original service name from the mapped name
                if dm_service in reverse_mapping:
                    original_service = reverse_mapping[dm_service]
                    #print(f"Mapped service {dm_service} to original service {original_service}.")

                    # Check if the original service is in renamed_containers and handle accordingly
                    if original_service in renamed_containers:
                        if comm_type == 'db':
                            # Store the service in db_values and then remove it from renamed_containers
                            db_values[original_service] = renamed_containers[original_service]
                            del renamed_containers[original_service]  # Delete after storing
                        elif comm_type == 'mc':
                            # Store the service in memcached_values and then remove it from renamed_containers
                            memcached_values[original_service] = renamed_containers[original_service]
                            del renamed_containers[original_service]  # Delete after storing
                #else:
                    
                    #print(f"Service {dm_service} NOT found in reverse mapping. Skipping.")

    return db_values, memcached_values

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

def create_logging_statefulset(apps_v1, namespace, redis_ip, storageclass):
    container = V1Container(
        name="logging-container",
        image=get_docker_image_with_pre_suffix("logging_capstone"),
        env=[client.V1EnvVar(name="REDIS_IP_ADDRESS", value=redis_ip)],
        ports=[client.V1ContainerPort(container_port=80)],
        image_pull_policy="Always"
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
                ),
                storage_class_name=storageclass
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

def wait_for_service_ready(v1, service_name, namespace, max_retries=5, delay=5):
    """
    Waits for the Kubernetes service to be ready before proceeding.
    """
    retries = 0
    while retries < max_retries:
        try:
            # Check if service is available
            service = v1.read_namespaced_service(service_name, namespace)
            print(f"Service {service_name} is ready on attempt {retries + 1}")
            return True  # Service is ready
        except client.exceptions.ApiException as e:
            print(f"Error details: {e}")
            if e.status == 404:
                print(f"Service {service_name} not found (attempt {retries + 1}/{max_retries})")
            else:
                print(f"Error checking service {service_name} (attempt {retries + 1}/{max_retries}): {e}")
            
            retries += 1
            time.sleep(delay)

    print(f"Service {service_name} is not ready after {max_retries} attempts.")
    return False

def create_db_service(v1, namespace, service_name):
    """
    Creates a headless service for the PostgreSQL StatefulSet.
    """
    print(f"Creating PostgreSQL service for {service_name}")
    print(f"{service_name}-headless-service")
    service = client.V1Service(
        metadata=V1ObjectMeta(name=f"{service_name}-headless-service", namespace=namespace, labels={"app": service_name}),
        spec=V1ServiceSpec(
            cluster_ip="None",  # Headless service for StatefulSet
            selector={"app": service_name},
            ports=[V1ServicePort(port=5432, target_port=5432, name='postgresql')]
        )
    )

    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Headless service for {service_name} created successfully.")

from kubernetes import client

def create_redis_insert_job(batch_v1, namespace, job_name, service_name):
    """
    Creates a Kubernetes Job to insert random data into Redis.
    """

    # Define the Python script to insert random data into Redis
    python_script = f"""\
import redis
import random
import time

def insert_random_data_into_redis(service_name, namespace):
    print(f"Inserting random data into Redis instance: {{service_name}}-master-0")

    redis_host = f"{{service_name}}-master-0.{{service_name}}-headless-service.{{namespace}}.svc.cluster.local"
    retries = 0
    max_retries = 5

    while retries < max_retries:
        try:
            # Connect to Redis
            r = redis.Redis(host=redis_host, port=6379, password="Redis", decode_responses=True)
            
            # Insert random data into Redis
            for _ in range(5):
                random_key = f"random_key_{{random.randint(1, 100)}}"
                random_value = f"RandomValue{{random.randint(1, 100)}}"
                r.set(random_key, random_value)
                print(f"Inserted {{random_key}}: {{random_value}} into Redis")

            print(f"Random data inserted into Redis instance: {{service_name}}-master-0")
            break

        except Exception as e:
            print(f"Error inserting data into {{service_name}}-master-0: {{e}}")
            retries += 1
            if retries < max_retries:
                time.sleep(5)

    if retries == max_retries:
        print(f"Failed to insert data into {{service_name}}-master-0 after {{max_retries}} retries.")

insert_random_data_into_redis("{service_name}", "{namespace}")
"""

    # Create a ConfigMap to hold the Python script
    config_map = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(name=f"{job_name}-script", namespace=namespace),
        data={"insert_script.py": python_script}
    )

    # Create the ConfigMap in the specified namespace
    core_v1 = client.CoreV1Api()
    core_v1.create_namespaced_config_map(namespace=namespace, body=config_map)

    # Define the Job container to run the Python script
    container = client.V1Container(
        name=job_name,
        image="python:3.9",  # Use a Python image to run the script
        command=["/bin/bash", "-c", "pip install redis && python /scripts/insert_script.py"],
        volume_mounts=[client.V1VolumeMount(mount_path="/scripts", name="script-volume")],
        image_pull_policy="IfNotPresent"
    )

    # Define the volume for the ConfigMap
    volume = client.V1Volume(
        name="script-volume",
        config_map=client.V1ConfigMapVolumeSource(name=f"{job_name}-script")
    )

    job_spec = client.V1JobSpec(
        template=client.V1PodTemplateSpec(
            spec=client.V1PodSpec(
                containers=[container],
                restart_policy="Never",
                volumes=[volume]
            )
        )
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
        spec=job_spec
    )

    batch_v1.create_namespaced_job(namespace=namespace, body=job)
    print(f"Kubernetes Job '{job_name}' created in namespace '{namespace}'.")


def create_postgres_insert_job(batch_v1, namespace, job_name, service_name):
    """
    Creates a Kubernetes Job to insert random data into PostgreSQL.
    """

    # Define the Python script to insert random data into the database
    python_script = f"""\
import psycopg2
import random
import time

def insert_random_data_into_db(service_name, namespace):
    print(f"Inserting random data into primary PostgreSQL instance: {{service_name}}-statefulset-0")

    db_host = f"{{service_name}}-primary-0.{{service_name}}-headless-service.{{namespace}}.svc.cluster.local"
    retries = 0
    max_retries = 5
    connection = None

    while retries < max_retries:
        try:
            connection = psycopg2.connect(
                user="user",
                password="password",
                host=db_host,
                port="5432",
                database="mydatabase"
            )
            cursor = connection.cursor()

            cursor.execute('''CREATE TABLE IF NOT EXISTS random_data (
                                id SERIAL PRIMARY KEY,
                                data VARCHAR(255)
                              );''')

            for _ in range(5):
                random_data = f"RandomData{{random.randint(1, 100)}}"
                cursor.execute(f"INSERT INTO random_data (data) VALUES ('{{random_data}}');")

            connection.commit()
            print(f"Random data inserted into primary PostgreSQL instance: {{service_name}}-statefulset-0")
            break

        except Exception as e:
            print(f"Error inserting data into {{service_name}}-statefulset-0: {{e}}")
            retries += 1
            if retries < max_retries:
                time.sleep(5)

        finally:
            if connection:
                cursor.close()
                connection.close()

    if retries == max_retries:
        print(f"Failed to insert data into {{service_name}}-statefulset-0 after {{max_retries}} retries.")

insert_random_data_into_db("{service_name}", "{namespace}")
"""

    # Create a ConfigMap to hold the Python script
    config_map = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(name=f"{job_name}-script", namespace=namespace),
        data={"insert_script.py": python_script}
    )

    # Create the ConfigMap in the specified namespace
    core_v1 = client.CoreV1Api()
    core_v1.create_namespaced_config_map(namespace=namespace, body=config_map)

    # Define the Job container to run the Python script
    container = client.V1Container(
        name=job_name,
        image="python:3.9",  # Use a Python image to run the script
        command=["/bin/bash", "-c", "pip install psycopg2-binary && python /scripts/insert_script.py"],
        volume_mounts=[client.V1VolumeMount(mount_path="/scripts", name="script-volume")],
        image_pull_policy="IfNotPresent"
    )

    # Define the volume for the ConfigMap
    volume = client.V1Volume(
        name="script-volume",
        config_map=client.V1ConfigMapVolumeSource(name=f"{job_name}-script")
    )

    job_spec = client.V1JobSpec(
        template=client.V1PodTemplateSpec(
            spec=client.V1PodSpec(
                containers=[container],
                restart_policy="Never",
                volumes=[volume]
            )
        )
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
        spec=job_spec
    )

    batch_v1.create_namespaced_job(namespace=namespace, body=job)
    print(f"Kubernetes Job '{job_name}' created in namespace '{namespace}'.")

def create_headless_service(v1, namespace, service_name):
    service = client.V1Service(
        metadata=V1ObjectMeta(name=service_name, namespace=namespace),
        spec=V1ServiceSpec(
            cluster_ip="None",  # This makes it headless
            selector={"app": f"{service_name}"},
            ports=[V1ServicePort(port=5432, target_port=5432)]
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Headless service '{service_name}' created.")

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

def create_pvc(v1, namespace, pvc_name, storage_class, access_mode=["ReadOnlyMany"], data_str=None):
    size="1Gi"
    if data_str:
        size = calculate_storage_size(data_str)

    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=pvc_name, namespace=namespace),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=access_mode,
            resources=client.V1ResourceRequirements(
                requests={'storage': size}
            ),
            storage_class_name=storage_class
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

def create_db_headless_service(v1, namespace, container_name):
    service = V1Service(
        metadata=V1ObjectMeta(name=f"{container_name}-headless-service", namespace=namespace, labels={"app": container_name}),
        spec=V1ServiceSpec(
            ports=[
                V1ServicePort(name="postgresql", port=5432, target_port=5432),
            ],
            selector={"app": container_name},
            cluster_ip="None"  # Makes the service headless
        )
    )
    response = v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Headless Kafka Service created in namespace '{namespace}'.")
    return response.metadata.name  # Return the name of the created service

def create_memcached_service(v1, namespace, container_name):
    service = V1Service(
        metadata=V1ObjectMeta(name=f"{container_name}-headless-service", namespace=namespace, labels={"app": container_name}),
        spec=V1ServiceSpec(
            ports=[
                V1ServicePort(name="redis", port=6379, target_port=6379),
            ],
            selector={"app": container_name},
            cluster_ip="None"  # Makes the service headless
        )
    )
    response = v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Headless Kafka Service created in namespace '{namespace}'.")
    return response.metadata.name  # Return the name of the created service

def create_postgres_statefulset(apps_v1, namespace, container_name, pvc_name, replicas=1):
    """
    Creates a PostgreSQL StatefulSet with primary-replica replication support using the Bitnami PostgreSQL image.
    It first creates the primary StatefulSet, then creates the replica StatefulSet.
    """
    lifecycle=V1Lifecycle(
        pre_stop=V1LifecycleHandler(
            _exec=V1ExecAction(
                command=["/bin/bash", "-c", "chown -R 65534:0 /bitnami/postgresql/data || true; chmod -R 777 /bitnami/postgresql/data"]
            )
        )
    )

    # Define the lifecycle with a postStop handler correctly
    primary_container = V1Container(
        name=f"{container_name}-primary",
        image="bitnami/postgresql:17",  # Use Bitnami PostgreSQL 17 image
        env=[
            V1EnvVar(name="POSTGRESQL_PASSWORD", value="password"),
            V1EnvVar(name="POSTGRESQL_USERNAME", value="user"),
            V1EnvVar(name="POSTGRESQL_DATABASE", value="mydatabase"),
            V1EnvVar(name="POSTGRESQL_REPLICATION_MODE", value="master"),  # Set as primary
            V1EnvVar(name="POSTGRESQL_REPLICATION_USER", value="repluser"),
            V1EnvVar(name="POSTGRESQL_REPLICATION_PASSWORD", value="replpassword"),
            # Logging configuration
            V1EnvVar(name="POSTGRESQL_LOGGING_COLLECTOR", value="on"),
            V1EnvVar(name="POSTGRESQL_LOG_MIN_MESSAGES", value="info"),
            V1EnvVar(name="POSTGRESQL_LOG_STATEMENT", value="all"),  # Log all SQL statements
        ],
        ports=[client.V1ContainerPort(container_port=5432)],  # PostgreSQL port
        volume_mounts=[
            V1VolumeMount(mount_path="/bitnami/postgresql/data", name="data-volume")  # Volume mount for PostgreSQL data
        ],
        image_pull_policy="IfNotPresent",
        lifecycle=lifecycle
    )

    init_container = V1Container(
        name="init-chown-data",
        image="bitnami/minideb",
        command=["/bin/bash", "-c", "chown -R 1001:1001 /bitnami/postgresql/data || true;"],
        volume_mounts=[
            V1VolumeMount(mount_path="/bitnami/postgresql/data", name="data-volume")
        ],
        security_context=V1SecurityContext(
            run_as_user=0,  # Running as root to change permissions
            run_as_group=0,
            privileged=True
        )
    )

    primary_volume = V1Volume(
        name="data-volume",
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name)
    )

    primary_pod_spec = V1PodSpec(
        init_containers=[init_container],
        containers=[primary_container], 
        volumes=[primary_volume],
    )
    primary_template = V1PodTemplateSpec(
        metadata=V1ObjectMeta(labels={"app": f"{container_name}", "service": f"{container_name}-primary"}),
        spec=primary_pod_spec
    )

    primary_stateful_set_spec = V1StatefulSetSpec(
        service_name=f"{container_name}-headless-service",
        replicas=1,  # Primary replicas (usually 1)
        selector=V1LabelSelector(match_labels={"app": f"{container_name}"}),
        template=primary_template,
        pod_management_policy="OrderedReady",
        update_strategy=client.V1StatefulSetUpdateStrategy(type="RollingUpdate")
    )

    primary_stateful_set = V1StatefulSet(
        metadata=V1ObjectMeta(name=f"{container_name}-primary", namespace=namespace),
        spec=primary_stateful_set_spec
    )

    # Create the primary StatefulSet
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=primary_stateful_set)
    print(f"Primary PostgreSQL StatefulSet '{container_name}-primary' created in namespace '{namespace}")
    replicas -= 1
    if replicas < 1:
        return 
    # Now create the replicas StatefulSet
    replica_container = V1Container(
        name=f"{container_name}-replica",
        image="bitnami/postgresql:17",  # Use Bitnami PostgreSQL 17 image
        env=[
            V1EnvVar(name="POSTGRESQL_PASSWORD", value="password"),
            V1EnvVar(name="POSTGRESQL_USERNAME", value="user"),
            V1EnvVar(name="POSTGRESQL_DATABASE", value="mydatabase"),
            V1EnvVar(name="POSTGRESQL_REPLICATION_MODE", value="slave"),  # Set as replica
            V1EnvVar(name="POSTGRESQL_MASTER_HOST", value=f"{container_name}-primary-0.{container_name}-headless-service.{namespace}.svc.cluster.local"),
            V1EnvVar(name="POSTGRESQL_MASTER_PORT_NUMBER", value="5432"),  # PostgreSQL port for replication
            V1EnvVar(name="POSTGRESQL_REPLICATION_USER", value="repluser"),
            V1EnvVar(name="POSTGRESQL_REPLICATION_PASSWORD", value="replpassword"),
            # Logging configuration
            V1EnvVar(name="POSTGRESQL_LOGGING_COLLECTOR", value="on"),
            V1EnvVar(name="POSTGRESQL_LOG_MIN_MESSAGES", value="info"),
            V1EnvVar(name="POSTGRESQL_LOG_STATEMENT", value="all"),  # Log all SQL statements
        ],
        ports=[client.V1ContainerPort(container_port=5432)],  # PostgreSQL port
        volume_mounts=[
            V1VolumeMount(mount_path="/bitnami/postgresql/data", name="data-volume")  # Volume mount for PostgreSQL data
        ],
        image_pull_policy="IfNotPresent",
        lifecycle=lifecycle
    )

    replica_volume = V1Volume(
        name="data-volume",
        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name)
    )

    replica_init_container = V1Container(
        name="init-chown-data",
        image="bitnami/minideb",
        command=["/bin/bash", "-c", "chown -R 1001:1001 /bitnami/postgresql/data && chmod -R 777 /bitnami/postgresql/data"],
        volume_mounts=[
            V1VolumeMount(mount_path="/bitnami/postgresql/data", name="data-volume")
        ],
        security_context=V1SecurityContext(
            run_as_user=65534,
            run_as_group=65534,
            privileged=True
        )
    )

    replica_pod_spec = V1PodSpec(
        init_containers=[init_container],
        containers=[replica_container], 
        volumes=[replica_volume]
    )

    replica_template = V1PodTemplateSpec(
        metadata=V1ObjectMeta(labels={"app": f"{container_name}", "service": f"{container_name}-replica"}),
        spec=replica_pod_spec
    )

    replica_stateful_set_spec = V1StatefulSetSpec(
        service_name=f"{container_name}-headless-service",
        replicas=replicas,  # Replica count
        selector=V1LabelSelector(match_labels={"app": f"{container_name}"}),
        template=replica_template,
        pod_management_policy="OrderedReady",
        update_strategy=client.V1StatefulSetUpdateStrategy(type="RollingUpdate")
    )

    replica_stateful_set = V1StatefulSet(
        metadata=V1ObjectMeta(name=f"{container_name}-replica", namespace=namespace),
        spec=replica_stateful_set_spec
    )

    # Create the replica StatefulSet
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=replica_stateful_set)
    print(f"Replica PostgreSQL StatefulSet '{container_name}-replica' created in namespace '{namespace}' with replicas.")

def create_redis_statefulset(apps_v1, namespace, container_name, pvc_name, replicas=1):
    """
    Creates a Redis StatefulSet with master-slave replication support.
    It first creates the master StatefulSet, then creates the replica StatefulSet.
    """

    # Redis master configuration
    master_container = client.V1Container(
        name=f"{container_name}-master",
        image="bitnami/redis:latest",  # Use Redis image
        ports=[client.V1ContainerPort(container_port=6379)],  # Redis port
        volume_mounts=[
            client.V1VolumeMount(mount_path="/data", name="data-volume")  # Volume mount for Redis data
        ],
        image_pull_policy="IfNotPresent",
        env=[
            client.V1EnvVar(name="REDIS_PASSWORD", value="Redis"),  # Redis port for replication
            client.V1EnvVar(name="REDIS_MASTER_PASSWORD", value="Redis"),  # Redis port for replication
            client.V1EnvVar(name="REDIS_REPLICATION_MODE", value="master"),
        ]
    )

    master_volume = client.V1Volume(
        name="data-volume",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name)
    )

    master_pod_spec = client.V1PodSpec(
        containers=[master_container],
        volumes=[master_volume],
    )

    master_template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": f"{container_name}", "service": f"{container_name}-master"}),
        spec=master_pod_spec
    )

    master_stateful_set_spec = client.V1StatefulSetSpec(
        service_name=f"{container_name}-headless-service",
        replicas=1,  # Master replicas (usually 1)
        selector=client.V1LabelSelector(match_labels={"app": f"{container_name}"}),
        template=master_template,
        pod_management_policy="OrderedReady",
        update_strategy=client.V1StatefulSetUpdateStrategy(type="RollingUpdate")
    )

    master_stateful_set = client.V1StatefulSet(
        metadata=client.V1ObjectMeta(name=f"{container_name}-master", namespace=namespace),
        spec=master_stateful_set_spec
    )

    # Create the master StatefulSet
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=master_stateful_set)
    print(f"Master Redis StatefulSet '{container_name}-master' created in namespace '{namespace}'")

    replicas -= 1
    if replicas < 1:
        return

    # Now create the replicas StatefulSet
    replica_container = client.V1Container(
        name=f"{container_name}-slave",
        image="bitnami/redis:latest",  # Use Redis image
        ports=[client.V1ContainerPort(container_port=6379)],  # Redis port
        volume_mounts=[
            client.V1VolumeMount(mount_path="/data", name="data-volume")  # Volume mount for Redis data
        ],
        image_pull_policy="IfNotPresent",
        env=[
            client.V1EnvVar(name="REDIS_PASSWORD", value="Redis"),  # Redis port for replication
            client.V1EnvVar(name="REDIS_MASTER_PASSWORD", value="Redis"),  # Redis port for replication
            client.V1EnvVar(name="REDIS_REPLICATION_MODE", value="slave"),
            client.V1EnvVar(name="REDIS_MASTER_HOST", value=f"{container_name}-master-0.{container_name}-headless-service.{namespace}.svc.cluster.local"),
            client.V1EnvVar(name="REDIS_MASTER_PORT", value="6379"),  # Redis port for replication
        ]
    )

    replica_volume = client.V1Volume(
        name="data-volume",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name)
    )

    replica_pod_spec = client.V1PodSpec(
        containers=[replica_container],
        volumes=[replica_volume],
    )

    replica_template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": f"{container_name}", "service": f"{container_name}-replica"}),
        spec=replica_pod_spec
    )

    replica_stateful_set_spec = client.V1StatefulSetSpec(
        service_name=f"{container_name}-headless-service",
        replicas=replicas,  # Replica count
        selector=client.V1LabelSelector(match_labels={"app": f"{container_name}"}),
        template=replica_template,
        pod_management_policy="OrderedReady",
        update_strategy=client.V1StatefulSetUpdateStrategy(type="RollingUpdate")
    )

    replica_stateful_set = client.V1StatefulSet(
        metadata=client.V1ObjectMeta(name=f"{container_name}-replica", namespace=namespace),
        spec=replica_stateful_set_spec
    )

    # Create the replica StatefulSet
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=replica_stateful_set)
    print(f"Replica Redis StatefulSet '{container_name}-replica' created in namespace '{namespace}' with {replicas} replicas.")

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


def segregate_receiving_services(calls_file, probabilities_file):
    # Load JSON files
    with open(calls_file, 'r') as calls_f, open(probabilities_file, 'r') as probs_f:
        calls_data = json.load(calls_f)
        probabilities_data = json.load(probs_f)
    
    # Create a dictionary to segregate services by received communication type
    segregated_services = {comm_type: [] for comm_type in ["http", "mc", "rpc", "db", "mq"]}

    # Iterate over the services in calls.json to identify receiving services
    for sender_service, timestamps in calls_data.items():
        for timestamp, calls in timestamps.items():
            for call in calls:
                receiver_service = call["dm_service"]
                communication_type = call["communication_type"]

                # Check if the receiver has the allowed communication type in probabilities.json
                allowed_types = probabilities_data.get(receiver_service, {})
                if communication_type in allowed_types:
                    segregated_services[communication_type].append(receiver_service)

    # Remove duplicates in each communication type's list
    for comm_type in segregated_services:
        segregated_services[comm_type] = list(set(segregated_services[comm_type]))

    return segregated_services

def filter_empty_slots(calls_data):
    """
    Filters out empty time slots and services with no valid calls.

    Parameters:
        calls_data (dict): The original calls.json data.

    Returns:
        dict: Filtered data with no empty time slots.
    """
    # Remove empty time slots for each service
    filtered_data = {
        service: {
            timestamp: calls for timestamp, calls in timestamps.items() if calls
        }
        for service, timestamps in calls_data.items()
    }

    # Remove services with no valid timestamps
    filtered_data = {service: ts for service, ts in filtered_data.items() if ts}

    return filtered_data


def generate_new_calls(calls_file, probabilities_file, output_file="new_calls.json"):
    """
    Generate new_calls.json based on probabilities.json and segregated services.
    
    Parameters:
        calls_file (str): Path to the calls.json file.
        probabilities_file (str): Path to the probabilities.json file.
        output_file (str): Path to save the generated new_calls.json file.
    """
    # Generate segregated services dynamically
    segregated_services = segregate_receiving_services(calls_file, probabilities_file)

    # Load input JSON files
    with open(calls_file, 'r') as calls_f, open(probabilities_file, 'r') as probs_f:
        calls_data = json.load(calls_f)
        probabilities_data = json.load(probs_f)

    # Initialize the new_calls structure
    new_calls = {}

    # Iterate over each service in calls.json
    for sender_service, timestamps in calls_data.items():
        new_calls[sender_service] = {}
        for timestamp in timestamps:
            new_calls[sender_service][timestamp] = []

            # Check if the service has defined probabilities
            if sender_service in probabilities_data:
                for comm_type, probability in probabilities_data[sender_service].items():
                    # Simulate whether the sender makes a call of this communication type
                    if random.random() < probability:
                        # Select a random service from the segregated services of this type
                        if comm_type in segregated_services and segregated_services[comm_type]:
                            receiver_service = random.choice(segregated_services[comm_type])
                            # Add the generated call to the new_calls
                            new_calls[sender_service][timestamp].append({
                                "dm_service": receiver_service,
                                "communication_type": comm_type
                            })
    
    # Filter out empty slots
    new_calls = filter_empty_slots(new_calls)  # Update new_calls with the filtered result

    # Save the generated new_calls to a file
    with open(output_file, 'w') as output_f:
        json.dump(new_calls, output_f, indent=4)
    print(f"New calls file saved to {output_file}")


def main():
    NAMESPACE = os.getenv("KUBERNETES_NAMESPACE", "static-application")
    KAFKA_EXTERNAL_GATEWAY_NODEPORT = int(os.getenv("KAFKA_EXTERNAL_GATEWAY_NODEPORT", "32092"))
    NODE_IP = os.getenv("NODE_IP", "localhost")
    STORAGE_CLASS = "nfs-client"
    config.load_kube_config()

    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()  # Correct API object for StatefulSets
    rbac_v1 = client.RbacAuthorizationV1Api()
    batch_v1 = client.BatchV1Api()

    get_or_create_namespace(NAMESPACE)

        # Menu for selecting normal or probabilistic model
    print("Select the model to use:")
    print("1. Normal model (using calls.json)")
    print("2. Probabilistic model (generate new_calls.json)")
    choice = input("Enter your choice (1/2): ").strip()

    calls_file = "calls_mapped.json"
    if choice == "2":
        # Generate new_calls.json based on probabilities.json
        probabilities_file = "probabilities.json"
        print("Generating new_calls.json using the probabilistic model...")
        #segregated_services = segregate_receiving_services(calls_file, probabilities_file)
        generate_new_calls(calls_file, probabilities_file)
        calls_file = "new_calls.json"  # Switch to using new_calls.json
        print("Generated new_calls.json successfully.")
    elif choice != "1":
        print("Invalid choice. Exiting.")
        return


    # Deploy Kafka and get kafka_replicas
    (kafka_replicas, kafka_statefulset_name, kafka_headless_service_name, kakfa_gateway_service_name) = deploy_kafka_environment(NAMESPACE, v1, apps_v1, rbac_v1, KAFKA_EXTERNAL_GATEWAY_NODEPORT)

    renamed_containers, calls = get_and_rename_containers(containersFile="containers.json", callsFile=calls_file)

    # Deploy Redis and get redis_ip
    deploy_redis_environment(NAMESPACE, v1, apps_v1)
    redis_service_name = 'redis-service'
    wait_for_pods_ready(NAMESPACE)

    # Call logging service setup (after Redis is ready)
    print(redis_service_name)
    create_logging_statefulset(apps_v1, NAMESPACE, redis_service_name, STORAGE_CLASS)
    create_logging_service(v1, NAMESPACE)

    # Get containers and calls data
    db_values, memcached_values = extract_remove_memcached_db_containers(renamed_containers, calls)
    
    # Random or round robin choice
    choice = input("Do you want random assignment of calls between instance IDs or round robin assignment?\n (Enter 0 for 'random' or 1 for 'round_robin'): ").strip().lower()

    # Define topics for Kafka (includes DB containers)
    topics = []

    # Handle other containers
    for container in renamed_containers:
        containerKeys = renamed_containers[container]
        mappedName = containerKeys['mappedName']
        replicas = containerKeys['replicas']
        topics.append({ "name": mappedName, "partitions": 1, "replication_factor": kafka_replicas })

        pvc_name = f"{mappedName}-pvc"
        job_name = f"{mappedName}-job"
        data = split_calls_to_replicas(calls.get(mappedName, {}), replicas, mappedName, choice)
        data_str = json.dumps(data).replace('"', '\\"')

        # Create PVC and Jobs for other containers
        create_pvc(v1, NAMESPACE, pvc_name, STORAGE_CLASS, data_str=data_str)
        create_jobs_with_data(batch_v1, NAMESPACE, job_name, pvc_name, data_str)

    # Handle DB containers differently
    for service_name, container_keys in memcached_values.items():
        memcached_mappedName = container_keys['mappedName']
        replicas = container_keys.get('replicas', 1)

        # Step 1: Create headless service for PostgreSQL container (for replication)
        create_memcached_service(v1, NAMESPACE, memcached_mappedName)
        create_container_service(v1, NAMESPACE, memcached_mappedName, [{ "port": 6379, "target_port": 6379, 'name': 'redis-port' }])

        # # Step 2: Create PostgreSQL StatefulSet with replication support
        pvc_name = f"{memcached_mappedName}-pvc"
        create_pvc(v1, NAMESPACE, pvc_name, STORAGE_CLASS, access_mode=["ReadWriteOnce"])
        create_redis_statefulset(apps_v1, NAMESPACE, memcached_mappedName, pvc_name, replicas=replicas)

    # Handle DB containers differently
    for service_name, container_keys in db_values.items():
        db_mappedName = container_keys['mappedName']
        replicas = container_keys.get('replicas', 1)

        # Step 1: Create headless service for PostgreSQL container (for replication)
        create_db_headless_service(v1, NAMESPACE, db_mappedName)
        create_container_service(v1, NAMESPACE, db_mappedName, [{ "port": 5432, "target_port": 5432, 'name': 'postgresql' }])

        # Step 2: Create PostgreSQL StatefulSet with replication support
        pvc_name = f"{db_mappedName}-pvc"
        create_pvc(v1, NAMESPACE, pvc_name, STORAGE_CLASS, access_mode=["ReadWriteOnce"])
        create_postgres_statefulset(apps_v1, NAMESPACE, db_mappedName, pvc_name, replicas=replicas)
    wait_for_all_jobs_to_complete(batch_v1, NAMESPACE)
    delete_all_configmaps(v1, NAMESPACE)
    delete_completed_jobs(batch_v1, v1, NAMESPACE)
    wait_for_pods_ready(NAMESPACE)
    for service_name, container_keys in memcached_values.items():
        db_mappedName = container_keys['mappedName']
        create_redis_insert_job(batch_v1, NAMESPACE, f"{db_mappedName}-insert-job", db_mappedName)

    for service_name, container_keys in db_values.items():
        db_mappedName = container_keys['mappedName']
        create_postgres_insert_job(batch_v1, NAMESPACE, f"{db_mappedName}-insert-job", db_mappedName)
    wait_for_all_jobs_to_complete(batch_v1, NAMESPACE)
    delete_all_configmaps(v1, NAMESPACE)
    delete_completed_jobs(batch_v1, v1, NAMESPACE)
    
    create_topics_http_request(topics, NAMESPACE, kafka_statefulset_name, kakfa_gateway_service_name, kafka_headless_service_name, KAFKA_EXTERNAL_GATEWAY_NODEPORT, NODE_IP)

    # Assign container jobs
    renamed_containers = addContainerJob(renamed_containers)

    # Handle non-DB containers
    for container_name in renamed_containers:
        containerKeys = renamed_containers[container_name]
        mappedName = containerKeys['mappedName']
        containerJob = containerKeys['containerJob']
        replicas = containerKeys['replicas']

        # Create services for each container
        create_container_service(v1, NAMESPACE, mappedName, [{ "port": 80, "target_port": 80, 'name': 'flask-service' }, { "port": 50051, "target_port": 50051, "name": 'grpc-service' }])

        pvc_name = f"{mappedName}-pvc"
        # Use apps_v1 for creating StatefulSets
        create_container_statefulset(apps_v1, NAMESPACE, mappedName, pvc_name, kafka_replicas, redis_ip=redis_service_name, container_job=containerJob, replicas=replicas)


    # Wait for all StatefulSets to be ready (including DB StatefulSets)
    wait_for_pods_ready(NAMESPACE)

    print("All statefulsets, deployments, and services are up in Kubernetes.")
    port_forward_and_exec_func(NAMESPACE, redis_service_name, 60892, 6379, funcToExec=set_start_time_redis)


if __name__ == "__main__":
    main()

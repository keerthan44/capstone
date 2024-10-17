import json
import re
import threading
import time
from kubernetes import client, config
import signal
import os
import subprocess
from kubernetes.client.rest import ApiException
import asyncio

def wait_for_pods_ready(namespace):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()

    def are_all_pods_ready(pods):
        return all(pod.status.phase == 'Running' for pod in pods)

    def are_all_deployments_ready():
        try:
            deployments = apps_v1.list_namespaced_deployment(namespace)
            for deployment in deployments.items:
                if deployment.status.replicas != deployment.status.ready_replicas:
                    return False
            return True
        except ApiException as e:
            print(f"Exception when listing deployments: {e}")
            return False

    def are_all_statefulsets_ready():
        try:
            statefulsets = apps_v1.list_namespaced_stateful_set(namespace)
            for statefulset in statefulsets.items:
                if statefulset.status.replicas != statefulset.status.ready_replicas:
                    return False
            return True
        except ApiException as e:
            print(f"Exception when listing statefulsets: {e}")
            return False

    while True:
        # Check if all pods are running
        pods = v1.list_namespaced_pod(namespace)
        if are_all_pods_ready(pods.items) and are_all_deployments_ready() and are_all_statefulsets_ready():
            print("All pods, deployments, and statefulsets are ready.")
            break
        
        print("Waiting for all pods, deployments, and statefulsets to be ready...")
        time.sleep(3)

def wait_for_service_ready(service_name, namespace):
    # Load kube config and create a client
    config.load_kube_config()
    v1 = client.CoreV1Api()

    while True:
        try:
            service = v1.read_namespaced_service(name=service_name, namespace=namespace)
            if service.status:
                print(f"Service '{service_name}' is available.")
                break
        except client.exceptions.ApiException as e:
            if e.status == 404:
                print(f"Service '{service_name}' not found, waiting...")
            else:
                print(f"Error fetching service status: {e}")
        
        time.sleep(1)

def port_forward_and_exec_func(namespace, service_name, local_port, remote_port, funcToExec=None, data={}):

    # Start port forwarding in a subprocess
    kill_process_on_port(local_port)
    print('service_name:', service_name)
    process = subprocess.Popen([
        'kubectl', 'port-forward', f'service/{service_name}', f'{local_port}:{remote_port}', '-n', namespace
    ])

    # Wait a few seconds to ensure port forwarding is established
    time.sleep(5)
    if funcToExec:
        funcToExec({**data, 'local_port': local_port})
    
    # Optionally, you can stop the port forwarding process if needed
    process.terminate()

def kill_process_on_port(local_port):
    # Find and kill the process using the local port
    try:
        # Use lsof to find the PID of the process using the port
        output = subprocess.check_output(['lsof', '-t', f'-i:{local_port}'])
        pids = output.decode().split()
        for pid in pids:
            os.kill(int(pid), signal.SIGKILL)
            print(f'Killed process with PID {pid} using port {local_port}')
    except subprocess.CalledProcessError:
        print(f'No process found using port {local_port}')

def get_or_create_namespace(namespace):
    # Load kube config and create a client
    config.load_kube_config()
    v1 = client.CoreV1Api()

    try:
        v1.read_namespace(name=namespace)
        print(f"Namespace '{namespace}' already exists.")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            v1.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
            print(f"Namespace '{namespace}' created.")
        else:
            raise

def return_ip_if_minikube():
    try:
        # Try to get the Minikube IP, which will only work if Minikube is running
        return subprocess.check_output(["minikube", "ip"]).decode().strip()
    except subprocess.CalledProcessError:
        # If the minikube command fails, it's likely not a Minikube environment
        print("Not running in Minikube.")
        return False

def get_external_ip_service(service_name, namespace='default'):
    try:
        # Load Kubernetes config
        config.load_kube_config()

        # Create a Kubernetes API client
        v1 = client.CoreV1Api()

        # Get the service object
        service = v1.read_namespaced_service(service_name, namespace)

        # Check if the service type is LoadBalancer
        if service.spec.type == "LoadBalancer":
            # Get the external IP for a regular cluster or minikube tunnel
            if service.status.load_balancer.ingress:
                for ingress in service.status.load_balancer.ingress:
                    if ingress.ip:
                        return ingress.ip
                return "External IP not assigned yet."
            else:
                return "External IP not available. Ensure 'minikube tunnel' is running."

        else:
            return "This service is not of type LoadBalancer."
    
    except ApiException as e:
        return f"Failed to get service information: {e}"

def get_minikube_service_ip_port(service_name, namespace):
    try:
        result = ""
        if os.uname().sysname == 'Darwin':
            is_minikube = input('Are you using Minikube(y/n): ')
            if is_minikube == 'y':
                print(f"Run this command and let it run in terminal.")
                print(f"minikube service {service_name} -n {namespace} --url")
                result = input("Enter the url: ")
        else:

        # Retrieve the URL of a Minikube service using the `minikube service` command
            result = subprocess.run(
                ["minikube", "service", service_name, "-n", namespace, "--url"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            result = result.stdout
        # The output should contain the full URL, e.g., http://<ip>:<port>
        url = result.strip()

        # Extract IP and port using regex
        match = re.match(r'http://([\d\.]+):(\d+)', url)
        
        if match:
            ip_address = match.group(1)
            port = match.group(2)
            print("Recieved IP address and port: ", ip_address, port)
            return ip_address, port
        else:
            print("Failed to parse URL")
            return None, None
    except subprocess.CalledProcessError as e:
        print(f"Failed to retrieve Minikube service IP: {e}")
        return None, None

def get_service_external_ip_forwarded_port(service_name, namespace=None, target_port=None, node_port_default=None):
    # Construct the kubectl command
    cmd = ['kubectl', 'get', 'svc', service_name, '-o', 'json']
    
    if namespace:
        cmd.extend(['-n', namespace])
    
    try:
        # Run the kubectl command
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse the JSON output
        svc_info = json.loads(result.stdout)
        
        # Extract the external IP (LoadBalancer IP or hostname)
        external_ip = svc_info.get('status', {}).get('loadBalancer', {}).get('ingress', [{}])[0].get('ip', None)
        
        # Find the matching node port for the specified target port
        for port_info in svc_info.get('spec', {}).get('ports', []):
            if port_info.get('port') == target_port:
                node_port = port_info.get('nodePort')
                break
        else:
            node_port = node_port_default
        
        if external_ip and node_port:
            print("Recieved ip address and port:", external_ip, node_port)
            return external_ip, node_port
        else:
            print("No ip address and port recieved")
            return None, None
    except subprocess.CalledProcessError as e:
        print(f"Failed to get service info: {e}")
        return None, None

def wait_for_job_completion(batch_v1, namespace, job_name):
    """
    Wait for a specific Kubernetes Job to complete.
    """
    while True:
        job_status = batch_v1.read_namespaced_job_status(job_name, namespace)
        if job_status.status.succeeded == 1:
            print(f"Job '{job_name}' completed successfully.")
            break
        elif job_status.status.failed:
            print(f"Job '{job_name}' failed.")
            break
        time.sleep(2)  # Check every 2 seconds

def wait_for_all_jobs_to_complete(batch_v1, namespace):
    while True:
        # List all Jobs in the specified namespace
        jobs = batch_v1.list_namespaced_job(namespace=namespace)
        all_jobs_completed = True
        
        for job in jobs.items:
            job_name = job.metadata.name
            job_status = job.status
            
            # Check if the job is completed
            if job_status.succeeded is None or job_status.succeeded == 0:
                print(f"Job '{job_name}' is not completed yet.")
                all_jobs_completed = False
        
        if all_jobs_completed:
            print("All jobs are completed.")
            return
        
        print("Waiting for all jobs to complete...")
        time.sleep(3)  # Sleep for 3 seconds before checking again

def delete_completed_jobs(batch_v1, v1_core, namespace):
    while True:
        # List all Jobs in the specified namespace
        jobs = batch_v1.list_namespaced_job(namespace=namespace)
        any_job_deleted = False
        
        for job in jobs.items:
            job_name = job.metadata.name
            job_status = job.status
            
            # Check if the job is completed
            if job_status.succeeded is not None and job_status.succeeded > 0:
                print(f"Deleting completed job '{job_name}'.")
                
                # Delete the Job
                batch_v1.delete_namespaced_job(name=job_name, namespace=namespace, body=client.V1DeleteOptions(propagation_policy='Foreground'))
        
        if not any_job_deleted:
            print("All completed jobs have been deleted.")
            break
        
        print("Waiting for more jobs to complete...")
        time.sleep(3)  # Sleep for 3 seconds before checking again
    
def get_docker_image_with_pre_suffix(name, pre=None, suffix=None):
    """
    Generate a Docker image string with an optional prefix and suffix.
    
    Args:
        name (str): The name of the Docker image.
        pre (str): Optional Docker image prefix (default: value from DOCKER_PREFIX environment variable).
        suffix (str): Optional Docker image suffix (default: value from DOCKER_SUFFIX environment variable).
        
    Returns:
        str: The formatted Docker image string.
    """
    # Use environment variables if pre or suffix are not provided
    pre = pre or os.environ.get('DOCKER_PREFIX')
    suffix = suffix or os.environ.get('DOCKER_SUFFIX')

    # Construct the image name with optional prefix and suffix
    prefix_part = f"{pre}/" if pre else ""
    suffix_part = f":{suffix}" if suffix else ""

    print(f"{prefix_part}{name}{suffix_part}")
    # Return the final Docker image string
    return f"{prefix_part}{name}{suffix_part}"


async def delete_configmap_async(core_v1, namespace, configmap_name):
    """
    Asynchronously delete a single ConfigMap by name in the specified namespace.
    
    Args:
        core_v1: The CoreV1Api instance for interacting with Kubernetes ConfigMaps.
        namespace: The namespace where the ConfigMap resides.
        configmap_name: The name of the ConfigMap to delete.
    """
    try:
        print(f"Deleting ConfigMap: {configmap_name}")
        core_v1.delete_namespaced_config_map(name=configmap_name, namespace=namespace)
        # Add a small delay to prevent overwhelming the API server
        await asyncio.sleep(0.1)
    except ApiException as e:
        print(f"Exception when deleting ConfigMap {configmap_name}: {e}")

async def delete_all_configmaps_async(core_v1, namespace):
    """
    Asynchronously delete all ConfigMaps in a specified namespace.
    
    Args:
        core_v1: The CoreV1Api instance for interacting with Kubernetes ConfigMaps.
        namespace: The namespace from which to delete all ConfigMaps.
    """
    try:
        # List all ConfigMaps in the specified namespace
        configmaps = core_v1.list_namespaced_config_map(namespace).items
        
        # Create asynchronous tasks for deleting all ConfigMaps
        delete_tasks = [
            delete_configmap_async(core_v1, namespace, configmap.metadata.name)
            for configmap in configmaps
        ]
        
        # Await the completion of all delete tasks
        await asyncio.gather(*delete_tasks)
        
        print(f"All ConfigMaps in namespace '{namespace}' have been deleted.")
    
    except ApiException as e:
        print(f"Exception when listing or deleting ConfigMaps: {e}")

def delete_all_configmaps(core_v1, namespace):
    """
    Synchronous wrapper for the asynchronous delete function to ensure it completes
    before proceeding with the next code.
    
    Args:
        core_v1: The CoreV1Api instance for interacting with Kubernetes ConfigMaps.
        namespace: The namespace from which to delete all ConfigMaps.
    """
    asyncio.run(delete_all_configmaps_async(core_v1, namespace))
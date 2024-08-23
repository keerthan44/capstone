import re
import time
from kubernetes import client, config
import signal
import os
import subprocess
from kubernetes.client.rest import ApiException

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

def get_or_create_namespace(namespace_name):
    # Load kube config and create a client
    config.load_kube_config()
    v1 = client.CoreV1Api()

    try:
        v1.read_namespace(name=namespace_name)
        print(f"Namespace '{namespace_name}' already exists.")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            v1.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace_name)))
            print(f"Namespace '{namespace_name}' created.")
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

def get_minikube_service_ip(service_name, namespace):
    # Retrieve the URL of a Minikube service using the `minikube service` command
    try:
        result = subprocess.run(
            ["minikube", "service", service_name, "-n", namespace, "--url"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # The output should contain the full URL, e.g., http://<ip>:<port>
        url = result.stdout.strip()

        # Extract the IP address from the URL using a regular expression
        match = re.search(r'http://([\d\.]+):\d+', url)
        if match:
            ip_address = match.group(1)
            return ip_address
        else:
            print("Failed to parse IP address from URL.")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Failed to retrieve Minikube service IP: {e}")
        return None
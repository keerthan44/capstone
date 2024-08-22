import time
from kubernetes import client, config
import signal
import os

import subprocess
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

import redis
import time
from kubernetes import client, config
from kubernetes.client import  V1Container, V1ObjectMeta, V1PodSpec, V1Service, V1ServiceSpec, V1ServicePort, V1Deployment, V1DeploymentSpec, V1PodTemplateSpec, V1LabelSelector

# Load the Kubernetes configuration
config.load_kube_config()

# Kubernetes API client
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()

def set_start_time_redis(data):
    r = redis.Redis(host='localhost', port=data['local_port'])
    print("Connected to Redis.")
    
    # Example operation: Get a value from Redis
    try:
        r.set('start_time', time.time_ns())  # Store time in miliseconds

        print("Redis Start Time value is now set at", r.get('start_time'))
        print("Containers will start communicating")
    except Exception as e:
        print(f"Error: {e}")

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
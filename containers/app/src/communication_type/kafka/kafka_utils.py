from kubernetes import client, config
from kubernetes.client.rest import ApiException
import sys

def get_kafka_brokers(namespace, kafka_statefulset_name, kafka_service_name='kafka'):
    """Retrieve Kafka broker addresses from the pods of a StatefulSet using the Kubernetes client library."""
    try:
        # Load the Kubernetes configuration
        config.load_incluster_config()  # Use this if running inside a Kubernetes cluster
        # config.load_kube_config()    # Use this if running outside a Kubernetes cluster
        
        # Initialize the AppsV1Api to interact with StatefulSets and CoreV1Api for pods
        apps_v1 = client.AppsV1Api()
        core_v1 = client.CoreV1Api()
        
        # Read the StatefulSet object
        statefulset = apps_v1.read_namespaced_stateful_set(
            name=kafka_statefulset_name, 
            namespace=namespace
        )
        
        # Extract pod names from the StatefulSet label selector
        label_selector = ",".join([f"{k}={v}" for k, v in statefulset.spec.selector.match_labels.items()])
        
        # Fetch the pods that belong to this StatefulSet
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector
        )
        
        # Extract brokers (pod IPs or DNS names) from the pod information
        brokers = set()
        for pod in pods.items:
            for container in pod.spec.containers:
                for port in container.ports:
                    # Use the pod's DNS name within the cluster
                    brokers.add(f"{pod.metadata.name}.{kafka_service_name}.{namespace}.svc.cluster.local:9092")
        
        if brokers:
            print(f"Kafka Brokers retrieved: {', '.join(brokers)}", file=sys.stderr)
            return list(brokers)
        else:
            print("No brokers found", file=sys.stderr)
            return []
        
    except ApiException as e:
        print(f"Error retrieving Kafka brokers: {e}", file=sys.stderr)
        return []

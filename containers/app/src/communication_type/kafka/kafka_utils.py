import asyncio
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import sys

def get_kafka_brokers(namespace, kafka_statefulset_name, kafka_replicas, kafka_service_name='kafka'):
    """Retrieve Kafka broker addresses from the pods of a StatefulSet using the Kubernetes client library."""
    try:
        brokers = []
        for i in range(kafka_replicas):
            brokers.append(f"{kafka_statefulset_name}-{i}.{kafka_service_name}.{namespace}.svc.cluster.local:9092")
        
        if brokers:
            print(f"Kafka Brokers retrieved: {', '.join(brokers)}", file=sys.stderr)
            return list(brokers)
        else:
            print("No brokers found", file=sys.stderr)
            return []
        
    except ApiException as e:
        print(f"Error retrieving Kafka brokers: {e}", file=sys.stderr)
        return []

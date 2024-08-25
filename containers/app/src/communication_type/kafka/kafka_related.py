import json
import multiprocessing
from confluent_kafka import Consumer, KafkaError, Producer
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import sys
from ..http.flask_server import make_http_call_to_logging_server

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



def produce_kafka_messages(kafka_namespace, kafka_statefulset_name, kafka_service_name, topic, message):
    """
    Produces a message to a Kafka topic.

    Args:
        kafka_namespace (str): The Kubernetes namespace where Kafka is deployed.
        kafka_statefulset_name (str): The name of the Kafka StatefulSet.
        kafka_service_name (str): The name of the Kafka service.
        topic (str): Kafka topic to produce messages to.
        message (dict): Message to produce, given as a dictionary.
    """
    bootstrap_servers = get_kafka_brokers(kafka_namespace, kafka_statefulset_name, kafka_service_name)
    if not bootstrap_servers:
        raise RuntimeError("No Kafka brokers found")
    
    # Kafka Producer configuration
    conf = {
        'bootstrap.servers': ",".join(bootstrap_servers)
    }

    # Create Producer instance
    producer = Producer(**conf)

    try:
        # Convert the message dictionary to a JSON string
        message_value = json.dumps(message)

        # Produce message
        producer.produce(topic, value=message_value)
        producer.flush()
        print(f"Message '{message_value}' sent to Kafka topic '{topic}'", file=sys.stderr)

    except:
        # Close down the producer cleanly
        print(f"Unable to send message", file=sys.stderr)

def consume_kafka_messages(kafka_namespace, kafka_statefulset_name, kafka_service_name, group_id, topic, dm_name, auto_offset_reset='earliest', timeout=1.0):
    """
    Consumes messages from a Kafka topic.

    Args:
        kafka_namespace (str): The Kubernetes namespace where Kafka is deployed.
        kafka_statefulset_name (str): The name of the Kafka StatefulSet.
        kafka_service_name (str): The name of the Kafka service.
        group_id (str): Consumer group ID.
        topic (str): Kafka topic to consume messages from.
        auto_offset_reset (str): Offset reset policy ('earliest' or 'latest').
        timeout (float): Timeout for polling messages in seconds.
    """
    bootstrap_servers = get_kafka_brokers(kafka_namespace, kafka_statefulset_name, kafka_service_name)
    if not bootstrap_servers:
        raise RuntimeError("No Kafka brokers found")
    
    # Kafka Consumer configuration
    conf = {
        'bootstrap.servers': ",".join(bootstrap_servers),
        'group.id': group_id,
        'auto.offset.reset': auto_offset_reset  # Start from the earliest message
    }

    # Create Consumer instance
    consumer = Consumer(**conf)

    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        print("Kafka Consumer is listening for messages...", file=sys.stderr)

        while True:
            msg = consumer.poll(timeout=timeout)

            if msg is None:
                continue

            if msg.error():
                print(f"Error: {msg.error()}")
                break
            else:
                # Deserialize JSON message back into a dictionary
                message_value = msg.value().decode('utf-8')
                message_dict = json.loads(message_value)

                make_http_call_to_logging_server(message_dict['um'], dm_name, message_dict['timestamp'], message_dict['communication_type']) 
                # Print or process the dictionary as needed
                print(f"Received message: {message_dict}", file=sys.stderr)

    finally:
        # Close down the consumer cleanly
        consumer.close()

def start_kafka_consumer_process(kafka_namespace, kafka_statefulset_name, kafka_service_name, group_id, topic, dm_name, auto_offset_reset='latest', timeout=1.0):
    """
    Starts the Kafka consumer in a separate process.

    Args:
        kafka_namespace (str): The Kubernetes namespace where Kafka is deployed.
        kafka_statefulset_name (str): The name of the Kafka StatefulSet.
        kafka_service_name (str): The name of the Kafka service.
        group_id (str): Consumer group ID.
        topic (str): Kafka topic to consume messages from.
        auto_offset_reset (str): Offset reset policy ('earliest' or 'latest').
        timeout (float): Timeout for polling messages in seconds.
    """
    # Start a new process to run the Kafka consumer
    consumer_process = multiprocessing.Process(
        target=consume_kafka_messages,
        args=(kafka_namespace, kafka_statefulset_name, kafka_service_name, group_id, topic, dm_name, auto_offset_reset, timeout)
    )
    consumer_process.start()

    return consumer_process
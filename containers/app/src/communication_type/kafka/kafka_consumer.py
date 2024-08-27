import json
import multiprocessing
from confluent_kafka import Consumer
import sys
from ..http.http_client import make_http_call_to_logging_server
from .kafka_utils import get_kafka_brokers

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

                make_http_call_to_logging_server(message_dict['um'], dm_name, message_dict['timestamp_sent'], message_dict['communication_type']) 
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
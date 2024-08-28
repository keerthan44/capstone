import asyncio
import multiprocessing
import json
import time
from confluent_kafka import Consumer, KafkaError
import sys
from ..http.http_client import make_http_call_to_logging_server
from .kafka_utils import get_kafka_brokers

async def consume_kafka_messages(kafka_namespace, kafka_statefulset_name, kafka_service_name, group_id, topic, dm_name, kafka_replicas, auto_offset_reset='earliest', timeout=1.0):
    """
    Consumes messages from a Kafka topic asynchronously.

    Args:
        kafka_namespace (str): The Kubernetes namespace where Kafka is deployed.
        kafka_statefulset_name (str): The name of the Kafka StatefulSet.
        kafka_service_name (str): The name of the Kafka service.
        group_id (str): Consumer group ID.
        topic (str): Kafka topic to consume messages from.
        auto_offset_reset (str): Offset reset policy ('earliest' or 'latest').
        timeout (float): Timeout for polling messages in seconds.
    """
    bootstrap_servers = get_kafka_brokers(kafka_namespace, kafka_statefulset_name, kafka_replicas, kafka_service_name)
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
            timestamp_received = str(time.time_ns() // 1_000_000)
            if msg.error():
                print(f"Error: {msg.error()}", file=sys.stderr)
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    break
            else:
                # Deserialize JSON message back into a dictionary
                message_value = msg.value().decode('utf-8')
                message_dict = json.loads(message_value)
                log_data = {
                    **message_dict,
                    'dm': dm_name,
                    'timestamp_received': timestamp_received
                }
                await make_http_call_to_logging_server(log_data)  # Await the async call
                # Print or process the dictionary as needed
                print(f"Received message: {message_dict}", file=sys.stderr)

    finally:
        # Close down the consumer cleanly
        consumer.close()

def kafka_consumer_process(kafka_namespace, kafka_statefulset_name, kafka_service_name, group_id, topic, dm_name, kafka_replicas, auto_offset_reset='latest', timeout=1.0):
    """
    Wrapper function to run the Kafka consumer inside an asyncio event loop in a separate process.

    Args:
        kafka_namespace (str): The Kubernetes namespace where Kafka is deployed.
        kafka_statefulset_name (str): The name of the Kafka StatefulSet.
        kafka_service_name (str): The name of the Kafka service.
        group_id (str): Consumer group ID.
        topic (str): Kafka topic to consume messages from.
        auto_offset_reset (str): Offset reset policy ('earliest' or 'latest').
        timeout (float): Timeout for polling messages in seconds.
    """
    asyncio.run(
        consume_kafka_messages(
            kafka_namespace, 
            kafka_statefulset_name, 
            kafka_service_name, 
            group_id, 
            topic, 
            dm_name, 
            kafka_replicas,
            auto_offset_reset, 
            timeout
        )
    )

def start_kafka_consumer_process(kafka_namespace, kafka_statefulset_name, kafka_service_name, group_id, topic, dm_name, kafka_replicas, auto_offset_reset='latest', timeout=1.0):
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
    # Start a new process to run the Kafka consumer asynchronously
    consumer_process = multiprocessing.Process(
        target=kafka_consumer_process,
        args=(kafka_namespace, kafka_statefulset_name, kafka_service_name, group_id, topic, dm_name, kafka_replicas, auto_offset_reset, timeout)
    )
    consumer_process.start()

    return consumer_process

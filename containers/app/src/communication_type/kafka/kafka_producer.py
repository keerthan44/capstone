import json
from confluent_kafka import Producer
import sys
from .kafka_utils import get_kafka_brokers

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


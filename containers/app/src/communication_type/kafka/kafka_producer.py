import json
import asyncio
from confluent_kafka import Producer
import sys
from .kafka_utils import get_kafka_brokers

async def produce_kafka_messages(kafka_namespace, kafka_statefulset_name, kafka_service_name, topic, message):
    """
    Asynchronously produces a message to a Kafka topic.

    Args:
        kafka_namespace (str): The Kubernetes namespace where Kafka is deployed.
        kafka_statefulset_name (str): The name of the Kafka StatefulSet.
        kafka_service_name (str): The name of the Kafka service.
        topic (str): Kafka topic to produce messages to.
        message (dict): Message to produce, given as a dictionary.
    """
    bootstrap_servers = await get_kafka_brokers(kafka_namespace, kafka_statefulset_name, kafka_service_name)
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

        loop = asyncio.get_running_loop()
        # Use the event loop's run_in_executor method to produce messages asynchronously
        await loop.run_in_executor(
            None,
            producer.produce,
            topic,
            message_value
        )
        
        # Flush the producer to ensure all messages are sent
        await loop.run_in_executor(None, producer.flush)

        print(f"Message '{message_value}' sent to Kafka topic '{topic}'", file=sys.stderr)

    except Exception as e:
        print(f"Unable to send message: {e}", file=sys.stderr)


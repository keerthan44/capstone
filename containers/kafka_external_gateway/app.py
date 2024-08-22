import time
from flask import Flask, request, jsonify
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import random
import sys

app = Flask(__name__)

def get_kafka_brokers(namespace, kafka_statefulset_name):
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
                    brokers.add(f"{pod.metadata.name}.kafka.{namespace}.svc.cluster.local:9092")
        
        if brokers:
            print(f"Kafka Brokers retrieved: {', '.join(brokers)}", file=sys.stderr)
            return list(brokers)
        else:
            print("No brokers found", file=sys.stderr)
            return None
        
    except ApiException as e:
        print(f"Error retrieving Kafka brokers: {e}", file=sys.stderr)
        return []

def get_random_broker(brokers):
    """Select a random Kafka broker from the list."""
    if not brokers:
        return None
    return random.choice(brokers)

def create_kafka_topics(broker, topics, timeout=60, poll_interval=5):
    # Initialize Kafka Admin Client
    admin_client = AdminClient({'bootstrap.servers': broker})

    # Define topics to create
    new_topics = [NewTopic(
        topic=topic['name'],
        num_partitions=topic.get('partitions', 1),
        replication_factor=topic.get('replication_factor', 1)
    ) for topic in topics]
    topic_names = [topic['name'] for topic in topics]

    # Create Topics
    try:
        # Initiate topic creation
        fs = admin_client.create_topics(new_topics, request_timeout=timeout)
        print(f"Kafka Topics creation initiated for: {', '.join(topic_names)}", file=sys.stderr)
    except Exception as e:
        print(f"Error creating Kafka topics: {e}", file=sys.stderr)
        return

    # Wait for Topics to be created
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Fetch existing topics
            existing_topics = set(admin_client.list_topics(timeout=10).topics.keys())

            # Check for missing topics
            missing_topics = [t for t in topic_names if t not in existing_topics]

            if not missing_topics:
                print(f"Kafka Topics created: {', '.join(topic_names)}", file=sys.stderr)
                return

            print(f"Waiting for topics to be created: {', '.join(missing_topics)}", file=sys.stderr)
        except Exception as e:
            print(f"Error while checking topics: {e}", file=sys.stderr)

        time.sleep(poll_interval)

    # Timeout reached
    existing_topics = set(admin_client.list_topics(timeout=10).topics.keys())
    missing_topics = [t for t in topic_names if t not in existing_topics]
    
    if missing_topics:
        print(f"Timeout reached. Topics not found: {', '.join(missing_topics)}", file=sys.stderr)

@app.route('/create_topics', methods=['POST'])
def create_topics():
    """Create multiple Kafka topics based on the request payload."""
    data = request.json
    topics = data.get('topics', [])
    namespace = data.get('namespace', 'default')
    kafka_statefulset_name = data.get('kafka_statefulset_name', 'kafka')
    timeout = data.get('timeout', 60)
    poll_interval = data.get('poll_interval', 5)

    brokers = get_kafka_brokers(namespace, kafka_statefulset_name)
    broker = get_random_broker(brokers)

    if not topics:
        return jsonify({'status': 'error', 'message': 'No topics provided'}), 400
    if not isinstance(topics, list):
        return jsonify({'status': 'error', 'message': 'Expected an array of dictionaries'}), 400
    if not all(isinstance(item, dict) for item in topics):
        return jsonify({'status': 'error', 'message': 'All items in the array should be dictionaries'}), 400

    if broker is None:
        return jsonify({'status': 'error', 'message': 'No brokers available'}), 500

    try:
        create_kafka_topics(broker, topics, timeout, poll_interval)
        return jsonify({'status': 'success', 'broker': broker})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/send_messages', methods=['POST'])
def send_messages():
    """Send multiple messages to Kafka topics based on the request payload."""
    data = request.json
    messages = data.get('messages', [])
    namespace = data.get('namespace', 'default')
    kafka_statefulset_name = data.get('kafka_statefulset_name', 'kafka')

    brokers = get_kafka_brokers(namespace, kafka_statefulset_name)
    broker = get_random_broker(brokers)
    
    if broker is None:
        return jsonify({'status': 'error', 'message': 'No brokers available'}), 500

    producer = Producer({'bootstrap.servers': broker})

    errors = []
    for message in messages:
        topic_name = message.get('topic')
        message_value = message.get('message')

        try:
            producer.produce(topic_name, value=message_value.encode('utf-8'))
        except Exception as e:
            errors.append({'topic': topic_name, 'message': message_value, 'error': str(e)})

    producer.flush()
    
    if errors:
        return jsonify({'status': 'partial_success', 'broker': broker, 'errors': errors}), 207
    return jsonify({'status': 'success', 'broker': broker})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
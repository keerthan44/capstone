from kubernetes import client, config
from kubernetes.client import V1Service, V1ObjectMeta, V1ServiceSpec, V1ServicePort, V1StatefulSet, V1StatefulSetSpec, V1PodTemplateSpec, V1PodSpec, V1Container, V1ContainerPort, V1PersistentVolumeClaim, V1EnvVar

# Load Kubernetes configuration
config.load_kube_config()
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()

def create_zookeeper_service(namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="zoo1", namespace=namespace, labels={"service": "zoo1-instance"}),
        spec=V1ServiceSpec(
            ports=[V1ServicePort(port=2181, target_port=2181)],
            selector={"service": "zoo1-instance"}
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Zookeeper Service created in namespace '{namespace}'.")

def create_zookeeper_statefulset(namespace):
    container = V1Container(
        name="zoo1-instance",
        image="wurstmeister/zookeeper",
        ports=[V1ContainerPort(container_port=2181)],
    )
    statefulset = V1StatefulSet(
        metadata=V1ObjectMeta(name="zoo1-instance", namespace=namespace, labels={"service": "zoo1-instance"}),
        spec=V1StatefulSetSpec(
            service_name="zoo1",
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"service": "zoo1-instance"}),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels={"service": "zoo1-instance"}),
                spec=V1PodSpec(
                    containers=[container]
                )
            )
        )
    )
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=statefulset)
    print(f"Zookeeper StatefulSet created in namespace '{namespace}'.")

def create_kafka_service(namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="kafka", namespace=namespace, labels={"service": "kafka-instance"}),
        spec=V1ServiceSpec(
            ports=[
                V1ServicePort(name="9092", port=9092, target_port=9092),
                V1ServicePort(name="9093", port=9093, target_port=9093),
                V1ServicePort(name="32092", port=32092, target_port=32092, node_port=32092)
            ],
            selector={"service": "kafka-instance"},
            type="NodePort"
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Kafka Service created in namespace '{namespace}'.")

def create_kafka_statefulset(namespace):
    container = V1Container(
        name="kafka-instance",
        image="wurstmeister/kafka",
        ports=[
            V1ContainerPort(container_port=9092),
            V1ContainerPort(container_port=9093),
            V1ContainerPort(container_port=32092)
        ],
        env=[
            V1EnvVar(name="MY_HOST_IP", value_from=client.V1EnvVarSource(field_ref=client.V1ObjectFieldSelector(field_path="status.hostIP"))),
            V1EnvVar(name="MY_POD_NAME", value_from=client.V1EnvVarSource(field_ref=client.V1ObjectFieldSelector(field_path="metadata.name"))),
            V1EnvVar(name="KAFKA_ADVERTISED_LISTENERS", value=f"INTERNAL://$(MY_POD_NAME).kafka.{namespace}.svc.cluster.local:9093,CLIENT://$(MY_POD_NAME).kafka.default.svc.cluster.local:9092,EXTERNAL://$(MY_HOST_IP):32092"),
            V1EnvVar(name="KAFKA_INTER_BROKER_LISTENER_NAME", value="INTERNAL"),
            V1EnvVar(name="KAFKA_LISTENERS", value="INTERNAL://:9093,CLIENT://:9092,EXTERNAL://:32092"),
            V1EnvVar(name="KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", value="INTERNAL:PLAINTEXT,CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT"),
            V1EnvVar(name="KAFKA_PORT", value="9092"),
            V1EnvVar(name="KAFKA_RESTART_ATTEMPTS", value="10"),
            V1EnvVar(name="KAFKA_RESTART_DELAY", value="5"),
            V1EnvVar(name="KAFKA_ZOOKEEPER_CONNECT", value="zoo1:2181"),
            V1EnvVar(name="KAFKA_ZOOKEEPER_SESSION_TIMEOUT", value="6000"),
            V1EnvVar(name="ZOOKEEPER_AUTOPURGE_PURGE_INTERVAL", value="0"),
        ]
    )
    statefulset = V1StatefulSet(
        metadata=V1ObjectMeta(name="kafka-instance", namespace=namespace, labels={"service": "kafka-instance"}),
        spec=V1StatefulSetSpec(
            service_name="kafka",
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"service": "kafka-instance"}),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels={"service": "kafka-instance"}),
                spec=V1PodSpec(
                    containers=[container]
                )
            )
        )
    )
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=statefulset)
    print(f"Kafka StatefulSet created in namespace '{namespace}'.")

from kubernetes import client, config
from kubernetes.client import V1Service, V1ObjectMeta, V1ServiceSpec, V1ServicePort
from confluent_kafka.admin import AdminClient as KafkaAdminClient, NewTopic
import time

# Load Kubernetes configuration
config.load_kube_config()
v1 = client.CoreV1Api()

def create_kafka_topic(broker, topic_names, num_partitions=1, replication_factor=1, timeout=60, poll_interval=5):
    # Initialize Kafka Admin Client
    admin_client = KafkaAdminClient(
        {"bootstrap.servers": broker}
    )

    # Define topics to create
    new_topics = [NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    ) for topic_name in topic_names]

    # Create Topics
    try:
        admin_client.create_topics(new_topics, validate_only=False)
        print(f"Kafka Topics creation initiated for: {', '.join(topic_names)}")
    except Exception as e:
        print(f"Error creating Kafka topics: {e}")
        return

    # Wait for Topics to be created
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Fetch existing topics
            cluster_metadata = admin_client.list_topics(timeout=10)
            existing_topics = cluster_metadata.topics.keys()  # Get the keys (topic names)

            # Check for missing topics
            missing_topics = [t for t in topic_names if t not in existing_topics]

            if not missing_topics:
                print(f"Kafka Topics created: {', '.join(topic_names)}")
                return

            print(f"Waiting for topics to be created: {', '.join(missing_topics)}")
        except Exception as e:
            print(f"Error while checking topics: {e}")

        time.sleep(poll_interval)

    # Timeout reached
    cluster_metadata = admin_client.list_topics(timeout=10)
    existing_topics = cluster_metadata.topics.keys()
    missing_topics = [t for t in topic_names if t not in existing_topics]
    
    if missing_topics:
        print(f"Timeout reached. Topics not found: {', '.join(missing_topics)}")


def main():
    namespace = "static-application"  # Replace with your namespace

    # # Deploy Zookeeper
    # create_zookeeper_service(namespace)
    # create_zookeeper_statefulset(namespace)

    # # Deploy Kafka
    # create_kafka_service(namespace)
    # create_kafka_statefulset(namespace)
    create_kafka_topic("192.168.49.2:32092", ["test-topic"])

if __name__ == "__main__":
    main()

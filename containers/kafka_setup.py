from kubernetes import client, config
from kubernetes.client import V1Service, V1ObjectMeta, V1ServiceSpec, V1ServicePort, V1StatefulSet, V1StatefulSetSpec, V1PodTemplateSpec, V1PodSpec, V1Container, V1ContainerPort, V1PersistentVolumeClaim, V1EnvVar
from confluent_kafka.admin import AdminClient as KafkaAdminClient, NewTopic
import time

# Load Kubernetes configuration
config.load_kube_config()
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()

def create_zookeeper_service(namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="zookeeper-service", namespace=namespace),
        spec=V1ServiceSpec(
            cluster_ip="None",  # Headless service
            ports=[V1ServicePort(port=2181, target_port=2181)],
            selector={"app": "zookeeper"}
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Zookeeper Service created in namespace '{namespace}'.")

def create_zookeeper_statefulset(namespace):
    container = V1Container(
        name="zookeeper",
        image="confluentinc/cp-zookeeper:7.3.2",
        ports=[V1ContainerPort(container_port=2181)],
        env=[
            V1EnvVar(name="ZOOKEEPER_CLIENT_PORT", value="2181"),
            V1EnvVar(name="ZOOKEEPER_DATA_DIR", value="/var/lib/zookeeper/data"),
            V1EnvVar(name="ZOOKEEPER_LOG_DIR", value="/var/lib/zookeeper/log"),
            V1EnvVar(name="ZOOKEEPER_SERVER_ID", value="1")
        ],
        volume_mounts=[
            {"mountPath": "/var/lib/zookeeper/data", "name": "zookeeper-data"},
            {"mountPath": "/var/lib/zookeeper/log", "name": "zookeeper-log"}
        ]
    )
    statefulset = V1StatefulSet(
        metadata=V1ObjectMeta(name="zookeeper-statefulset", namespace=namespace),
        spec=V1StatefulSetSpec(
            service_name="zookeeper-service",
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"app": "zookeeper"}),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels={"app": "zookeeper"}),
                spec=V1PodSpec(
                    containers=[container],
                    security_context=client.V1PodSecurityContext(fs_group=1000),
                )
            ),
            volume_claim_templates=[
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(name="zookeeper-data"),
                    spec=client.V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=client.V1ResourceRequirements(
                            requests={"storage": "1Gi"}
                        )
                    )
                ),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(name="zookeeper-log"),
                    spec=client.V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=client.V1ResourceRequirements(
                            requests={"storage": "1Gi"}
                        )
                    )
                )
            ]
        )
    )
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=statefulset)
    print(f"Zookeeper StatefulSet created in namespace '{namespace}'.")

def create_kafka_service(namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="kafka-service", namespace=namespace),
        spec=V1ServiceSpec(
            cluster_ip="None",  # Headless service
            ports=[
                V1ServicePort(name="internal", port=29092, target_port=29092),
                V1ServicePort(name="external", port=30092, target_port=9092)
            ],
            selector={"app": "kafka"}
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Kafka Service created in namespace '{namespace}'.")

def create_kafka_statefulset(namespace):
    container = V1Container(
        name="kafka",
        image="confluentinc/cp-kafka:7.0.1",
        ports=[
            V1ContainerPort(container_port=29092),
            V1ContainerPort(container_port=9092)
        ],
        env=[
            V1EnvVar(name="KAFKA_ADVERTISED_LISTENERS", value="INTERNAL://:29092,EXTERNAL://:9092"),
            V1EnvVar(name="KAFKA_LISTENERS", value="INTERNAL://:29092,EXTERNAL://:9092"),
            V1EnvVar(name="KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", value="INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"),
            V1EnvVar(name="KAFKA_ZOOKEEPER_CONNECT", value="zookeeper-service:2181"),
            V1EnvVar(name="KAFKA_AUTO_CREATE_TOPICS_ENABLE", value="true"),
            V1EnvVar(name="KAFKA_INTER_BROKER_LISTENER_NAME", value="INTERNAL")
        ],
        volume_mounts=[
            {"mountPath": "/var/lib/kafka", "name": "kafka-data"}
        ]
    )
    statefulset = V1StatefulSet(
        metadata=V1ObjectMeta(name="kafka-statefulset", namespace=namespace),
        spec=V1StatefulSetSpec(
            service_name="kafka-service",
            replicas=3,
            selector=client.V1LabelSelector(match_labels={"app": "kafka"}),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels={"app": "kafka"}),
                spec=V1PodSpec(
                    containers=[container],
                    security_context=client.V1PodSecurityContext(fs_group=1000),
                )
            ),
            volume_claim_templates=[
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(name="kafka-data"),
                    spec=client.V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=client.V1ResourceRequirements(
                            requests={"storage": "1Gi"}
                        )
                    )
                )
            ]
        )
    )
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=statefulset)
    print(f"Kafka StatefulSet created in namespace '{namespace}'.")

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
            existing_topics = admin_client.list_topics()
            missing_topics = [t for t in topic_names if t not in existing_topics]

            if not missing_topics:
                print(f"Kafka Topics created: {', '.join(topic_names)}")
                return

            print(f"Waiting for topics to be created: {', '.join(missing_topics)}")
        except Exception as e:
            print(f"Error while checking topics: {e}")

        time.sleep(poll_interval)

    # Timeout reached
    missing_topics = [t for t in topic_names if t not in admin_client.list_topics()]
    if missing_topics:
        print(f"Timeout reached. Topics not found: {', '.join(missing_topics)}")

def main():
    namespace = "default"  # Replace with your namespace
    broker = "192.168.49.2:30092"  # Replace with your Kafka external service endpoint

    # create_zookeeper_service(namespace)
    # create_zookeeper_statefulset(namespace)
    # time.sleep(30)  # Wait for Zookeeper to start up
    # create_kafka_service(namespace)
    # create_kafka_statefulset(namespace)
    # time.sleep(60)  # Wait for Kafka to start up

    # Define topic names
    topic_names = ["my-topic1", "my-topic2"]
    create_kafka_topic(broker, topic_names)

if __name__ == "__main__":
    main()

import time
from kubernetes import client, config
from kubernetes.client import V1Service, V1ObjectMeta, V1ServiceSpec, V1ServicePort, V1StatefulSet, V1StatefulSetSpec, V1PodTemplateSpec, V1PodSpec, V1Container, V1ContainerPort, V1PersistentVolumeClaim, V1EnvVar
from kubernetes.client.rest import ApiException
from utils import get_service_external_ip_forwarded_port, wait_for_pods_ready, get_minikube_service_ip_port

import os
import requests

def create_zookeeper_service(v1, namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="zoo1", namespace=namespace, labels={"service": "zoo1-instance"}),
        spec=V1ServiceSpec(
            ports=[V1ServicePort(port=2181, target_port=2181)],
            selector={"service": "zoo1-instance"}
        )
    )
    v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Zookeeper Service created in namespace '{namespace}'.")

def create_zookeeper_statefulset(apps_v1, namespace):
    container = V1Container(
        name="zoo1-instance",
        image="wurstmeister/zookeeper",
        ports=[V1ContainerPort(container_port=2181)],
        image_pull_policy="IfNotPresent",  # Set the image pull policy to IfNotPresent
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

def create_kafka_headless_service(v1, namespace):
    service = V1Service(
        metadata=V1ObjectMeta(name="kafka", namespace=namespace, labels={"service": "kafka-instance"}),
        spec=V1ServiceSpec(
            ports=[
                V1ServicePort(name="9092", port=9092, target_port=9092),
                V1ServicePort(name="9093", port=9093, target_port=9093)
            ],
            selector={"service": "kafka-instance"},
            cluster_ip="None"  # Makes the service headless
        )
    )
    response = v1.create_namespaced_service(namespace=namespace, body=service)
    print(f"Headless Kafka Service created in namespace '{namespace}'.")
    return response.metadata.name  # Return the name of the created service


def create_kafka_statefulset(apps_v1, namespace):
    # Ask the user for the number of replicas
    replicas = int(input("Enter the number of Kafka replicas: "))

    container = V1Container(
        name="kafka-instance",
        image="wurstmeister/kafka",
        image_pull_policy="IfNotPresent",  # Set the image pull policy to IfNotPresent
        ports=[
            V1ContainerPort(container_port=9092),
            V1ContainerPort(container_port=9093)
        ],
        env=[
            V1EnvVar(name="MY_POD_NAME", value_from=client.V1EnvVarSource(field_ref=client.V1ObjectFieldSelector(field_path="metadata.name"))),
            V1EnvVar(name="KAFKA_ADVERTISED_LISTENERS", value=f"INTERNAL://$(MY_POD_NAME).kafka.{namespace}.svc.cluster.local:9093,CLIENT://$(MY_POD_NAME).kafka.{namespace}.svc.cluster.local:9092"),
            V1EnvVar(name="KAFKA_INTER_BROKER_LISTENER_NAME", value="INTERNAL"),
            V1EnvVar(name="KAFKA_LISTENERS", value="INTERNAL://:9093,CLIENT://:9092"),
            V1EnvVar(name="KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", value="INTERNAL:PLAINTEXT,CLIENT:PLAINTEXT"),
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
            replicas=replicas,
            selector=client.V1LabelSelector(match_labels={"service": "kafka-instance"}),
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(labels={"service": "kafka-instance"}),
                spec=V1PodSpec(
                    containers=[container]
                )
            )
        )
    )
    
    response = apps_v1.create_namespaced_stateful_set(namespace=namespace, body=statefulset)
    print(f"Kafka StatefulSet with {replicas} replica(s) created in namespace '{namespace}'.")
    return replicas, response.metadata.name


def create_kafka_external_gateway_deployment(apps_v1, namespace):
    replicas = int(input("Enter the number of Kafka External Gateway replicas: "))
    """Create a Kubernetes Deployment."""
    container = client.V1Container(
        name="kafka-external-gateway",
        image='kafka-external-gateway:latest',
        ports=[client.V1ContainerPort(container_port=8080)],
        env=[
            client.V1EnvVar(name="KAFKA_BROKER", value="localhost:9092")
        ],
        image_pull_policy="IfNotPresent",  # Set the image pull policy to IfNotPresent
    )
    spec = client.V1PodSpec(
        containers=[container]
    )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "kafka-external-gateway"}),
        spec=spec
    )
    deployment_spec = client.V1DeploymentSpec(
        replicas=replicas,
        selector=client.V1LabelSelector(match_labels={"app": "kafka-external-gateway"}),
        template=template
    )
    deployment = client.V1Deployment(
        metadata=client.V1ObjectMeta(name="kafka-external-gateway", namespace=namespace),
        spec=deployment_spec
    )
    try:
        apps_v1.create_namespaced_deployment(namespace=namespace, body=deployment)
        print(f"Deployment created in namespace '{namespace}'.")
    except ApiException as e:
        print(f"Exception when creating Deployment: {e}")

def create_kafka_external_gateway_service(v1, namespace, kafka_external_gateway_nodeport):
    """Create a Kubernetes Service with LoadBalancer type."""
    service_spec = client.V1ServiceSpec(
        selector={"app": "kafka-external-gateway"},
        ports=[client.V1ServicePort(port=80, target_port=8080, node_port=kafka_external_gateway_nodeport)],
        type="LoadBalancer"
    )
    service = client.V1Service(
        metadata=client.V1ObjectMeta(name="kafka-external-gateway", namespace=namespace),
        spec=service_spec
    )
    try:
        response = v1.create_namespaced_service(namespace=namespace, body=service)
        print(f"Service created in namespace '{namespace}'.")
        return response.metadata.name  # Return the name of the created service
    except ApiException as e:
        print(f"Exception when creating Service: {e}")
        return None
    
def create_or_update_kafka_external_gateway_role_and_rolebinding(rbac_v1, namespace):
    """Create or update a Kubernetes Role and RoleBinding for managing Kafka External Gateway resources."""
    # # Load kube config
    # config.load_kube_config()

    # # Create RBAC API client
    # rbac_v1 = client.RbacAuthorizationV1Api()

    # Define the Role
    role = client.V1Role(
        metadata=client.V1ObjectMeta(name="kafka-external-gateway-manager", namespace=namespace),
        rules=[
            client.V1PolicyRule(
                api_groups=["apps"],  # The api group for StatefulSets is "apps"
                resources=["statefulsets"],
                verbs=["create", "get", "list", "watch", "update", "delete"]
            ),
            client.V1PolicyRule(
                api_groups=[""],  # The api group for Pods is ""
                resources=["pods"],
                verbs=["create", "get", "list", "watch", "update", "delete"]
            )
        ]
    )
    
    # Define the RoleBinding
    role_binding = client.V1RoleBinding(
        metadata=client.V1ObjectMeta(name="kafka-external-gateway-manager-binding", namespace=namespace),
        subjects=[
            client.RbacV1Subject(
                kind="ServiceAccount",
                name="default",
                namespace=namespace
            )
        ],
        role_ref=client.V1RoleRef(
            kind="Role",
            name="kafka-external-gateway-manager",
            api_group="rbac.authorization.k8s.io"
        )
    )
    
    # Try to update or create Role
    try:
        rbac_v1.replace_namespaced_role(name=role.metadata.name, namespace=namespace, body=role)
        print(f"Role '{role.metadata.name}' updated in namespace '{namespace}'.")
    except ApiException as e:
        if e.status == 404:  # Not Found, create the role
            rbac_v1.create_namespaced_role(namespace=namespace, body=role)
            print(f"Role '{role.metadata.name}' created in namespace '{namespace}'.")
        else:
            print(f"Exception when creating or updating Role: {e}")
            raise

    # Try to update or create RoleBinding
    try:
        rbac_v1.replace_namespaced_role_binding(name=role_binding.metadata.name, namespace=namespace, body=role_binding)
        print(f"RoleBinding '{role_binding.metadata.name}' updated in namespace '{namespace}'.")
    except ApiException as e:
        if e.status == 404:  # Not Found, create the role_binding
            rbac_v1.create_namespaced_role_binding(namespace=namespace, body=role_binding)
            print(f"RoleBinding '{role_binding.metadata.name}' created in namespace '{namespace}'.")
        else:
            print(f"Exception when creating or updating RoleBinding: {e}")
            raise


def create_topics_http_request(topics, namespace, kafka_statefulset_name, kafka_external_gateway_name ,kafka_service_name, kafka_external_gateway_nodeport, timeout=60, poll_interval=5, retries=3):
    kafka_gateway_ip_port = get_minikube_service_ip_port(kafka_external_gateway_name, namespace)
    if not kafka_gateway_ip_port[0] or not kafka_gateway_ip_port[1]:
        kafka_gateway_ip_port = get_service_external_ip_forwarded_port(kafka_external_gateway_name, namespace, target_port=80, node_port_default=kafka_external_gateway_nodeport)
    kafka_gateway_ip, kafka_gateway_port = kafka_gateway_ip_port
    if not kafka_gateway_ip or not kafka_gateway_port:
        raise RuntimeError("Failed to get Kafka External Gateway IP and port.")
    
    data = {
        "topics": topics,
        "namespace": namespace,
        "kafka_statefulset_name": kafka_statefulset_name,
        "kafka_service_name": kafka_service_name,
        "timeout": timeout,
        "poll_interval": poll_interval
    }
    url = f"http://{kafka_gateway_ip}:{kafka_gateway_port}/create_topics"
    
    attempt = 1
    while True:
        print(f"Attempt {attempt} to send HTTP request to create topics.")
        try:
            response = requests.post(url, json=data)
            if response.status_code == 200:
                print("Request successful:", response.json())
                return  # Exit the function if request is successful
            else:
                print(f"Failed to create topics. Status code: {response.status_code}, Response: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        time.sleep(10)
        attempt += 1


def deploy_kafka_environment(namespace, v1, apps_v1, rbac_v1, kafka_external_gateway_nodeport):
    # Deploy Zookeeper
    create_zookeeper_service(v1, namespace)
    create_zookeeper_statefulset(apps_v1, namespace)
    wait_for_pods_ready(namespace)
    # Deploy Kafka
    kafka_headless_service_name = create_kafka_headless_service(v1, namespace)
    (kafka_replicas, kafka_statefulset_name) = create_kafka_statefulset(apps_v1, namespace)

    # Deploy Kafka External Gateway
    create_or_update_kafka_external_gateway_role_and_rolebinding(rbac_v1, namespace)
    kafka_gateway_service_name = create_kafka_external_gateway_service(v1, namespace, kafka_external_gateway_nodeport)
    create_kafka_external_gateway_deployment(apps_v1, namespace)
    return (kafka_replicas, kafka_statefulset_name, kafka_headless_service_name, kafka_gateway_service_name)


def main():
# Load Kubernetes configuration
    config.load_kube_config()
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    rbac_v1 = client.RbacAuthorizationV1Api()
    namespace = "static-application"  # Replace with your namespace

    create_zookeeper_service(v1, namespace)
    create_zookeeper_statefulset(apps_v1, namespace)
    wait_for_pods_ready(namespace)

    # Deploy Kafka
    create_kafka_headless_service(v1, namespace)
    create_kafka_statefulset(apps_v1, namespace)

    # Deploy Kafka External Gateway
    create_or_update_kafka_external_gateway_role_and_rolebinding(rbac_v1, namespace)
    create_kafka_external_gateway_service(v1, namespace, 32092)
    create_kafka_external_gateway_deployment(apps_v1, namespace)

if __name__ == "__main__":
    main()
    # create_topics_http_request([{"name": "test-topic"}], "static-application", "kafka-instance", "kafka-external-gateway", 32092)

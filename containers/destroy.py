import asyncio
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import os
from dotenv import load_dotenv
import sys
import time

load_dotenv()  # Load environment variables from .env.

# Load Kubernetes configuration (either incluster or kubeconfig)
config.load_kube_config()  # Use this if running outside a Kubernetes cluster

# Kubernetes API clients
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()
batch_v1 = client.BatchV1Api()
networking_v1 = client.NetworkingV1Api()
policy_v1 = client.PolicyV1Api()
rbac_v1 = client.RbacAuthorizationV1Api()


async def delete_resource(api_instance, delete_func, resource_type, resource_name, namespace):
    """Delete a specific resource type in a namespace asynchronously."""
    try:
        delete_func(name=resource_name, namespace=namespace)
        print(f"Deleted {resource_type} {resource_name} in namespace {namespace}")
    except ApiException as e:
        print(f"Failed to delete {resource_type} {resource_name}: {e}", file=sys.stderr)

async def delete_all_resources(namespace):
    """Delete all types of resources in the namespace asynchronously."""
    tasks = []
    # Resources to delete
    resource_types = {
        'persistentvolumeclaim': (v1, v1.delete_namespaced_persistent_volume_claim, v1.list_namespaced_persistent_volume_claim),
        'service': (v1, v1.delete_namespaced_service, v1.list_namespaced_service),
        'replicaset': (apps_v1, apps_v1.delete_namespaced_replica_set, apps_v1.list_namespaced_replica_set),
        'deployment': (apps_v1, apps_v1.delete_namespaced_deployment, apps_v1.list_namespaced_deployment),  # Include Deployments
        'statefulset': (apps_v1, apps_v1.delete_namespaced_stateful_set, apps_v1.list_namespaced_stateful_set),
        'daemonset': (apps_v1, apps_v1.delete_namespaced_daemon_set, apps_v1.list_namespaced_daemon_set),
        'configmap': (v1, v1.delete_namespaced_config_map, v1.list_namespaced_config_map),
        'secret': (v1, v1.delete_namespaced_secret, v1.list_namespaced_secret),
        'job': (batch_v1, batch_v1.delete_namespaced_job, batch_v1.list_namespaced_job),
        'cronjob': (batch_v1, batch_v1.delete_namespaced_cron_job, batch_v1.list_namespaced_cron_job),  # Correct API for CronJobs
        'networkpolicy': (networking_v1, networking_v1.delete_namespaced_network_policy, networking_v1.list_namespaced_network_policy),
        'poddisruptionbudget': (policy_v1, policy_v1.delete_namespaced_pod_disruption_budget, policy_v1.list_namespaced_pod_disruption_budget),
        'role': (rbac_v1, rbac_v1.delete_namespaced_role, rbac_v1.list_namespaced_role),
    }

    # List and delete each resource type
    for resource_type, (api_instance, delete_func, list_func) in resource_types.items():
        try:
            # Fetch all resources of this type in the namespace

            resources = list_func(namespace=namespace).items

            # Create async tasks for deleting the resources
            if resources:
                for resource in resources:
                    tasks.append(delete_resource(api_instance, delete_func, resource_type, resource.metadata.name, namespace))
            else:
                print(f"Skipping unknown resource type: {resource_type}", file=sys.stderr)
        except ApiException as e:
            print(f"Error fetching {resource_type}s: {e}", file=sys.stderr)

    # Run deletion tasks concurrently
    await asyncio.gather(*tasks)

async def delete_remaining_pods(namespace):
    """Explicitly delete any remaining pods in the namespace."""
    try:
        remaining_pods = v1.list_namespaced_pod(namespace=namespace).items
        if remaining_pods:
            print(f"Deleting remaining pods in namespace {namespace}...")
            tasks = [delete_resource(v1, v1.delete_namespaced_pod, 'pod', pod.metadata.name, namespace)
                     for pod in remaining_pods]
            await asyncio.gather(*tasks)
    except ApiException as e:
        print(f"Failed to delete remaining pods: {e}", file=sys.stderr)

async def ensure_namespace_deleted(namespace):
    """Ensure that the namespace is fully deleted and all resources are removed."""
    # Delete all resources inside the namespace
    await delete_all_resources(namespace)

    # Wait for some time to ensure resources are terminated
    time.sleep(5)

    # Check and delete remaining pods explicitly if any are left
    await delete_remaining_pods(namespace)

def check_active_pods(namespace):
    """Check if there are any active pods in the namespace."""
    try:
        # List all pods in the specified namespace
        while True:
            active_pods = v1.list_namespaced_pod(namespace=namespace)
            if active_pods.items:
                print("Waiting for pods to terminate")
                time.sleep(2)
            else:
                break
    except ApiException as e:
        print(f"Failed to check active pods: {e}", file=sys.stderr)
        return False

if __name__ == '__main__':
    namespace = os.getenv("KUBERNETES_NAMESPACE", 'static-application')  # Specify the namespace to delete resources from

    # Ensure that the namespace is completely deleted
    asyncio.run(ensure_namespace_deleted(namespace))
    check_active_pods(namespace)

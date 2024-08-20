import logging
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_namespace(namespace_name, timeout=300, interval=5):
    # Load the kube config from default location
    config.load_kube_config()
    
    # Create an instance of the CoreV1Api
    v1 = client.CoreV1Api()
    
    try:
        # Check if the namespace exists
        namespaces = v1.list_namespace()
        namespace_names = [ns.metadata.name for ns in namespaces.items]
        
        if namespace_name not in namespace_names:
            logger.warning(f"Namespace '{namespace_name}' does not exist.")
            return
        
        # Attempt to delete the namespace
        logger.info(f"Attempting to delete namespace '{namespace_name}'")
        v1.delete_namespace(name=namespace_name)
        
        # Wait until the namespace is deleted
        start_time = time.time()
        while True:
            namespaces = v1.list_namespace()
            namespace_names = [ns.metadata.name for ns in namespaces.items]
            
            if namespace_name not in namespace_names:
                logger.info(f"Namespace '{namespace_name}' has been deleted.")
                break
            
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                logger.error(f"Timeout reached. Namespace '{namespace_name}' is still present.")
                break
            
            logger.info(f"Waiting for namespace '{namespace_name}' to be deleted...")
            time.sleep(interval)
        
    except ApiException as e:
        # Log detailed error information
        logger.error(f"An error occurred: {e}")
        logger.error(f"HTTP response headers: {e.headers}")
        logger.error(f"HTTP response body: {e.body}")

if __name__ == "__main__":
    delete_namespace(os.getenv("KUBERNETES_NAMESPACE", 'static-application'))

import time
import random
import redis.asyncio as redis
from ..http.http_client import make_http_call_to_logging_server

async def redis_crud_with_logging(service_name, namespace, json_data):
    # List of CRUD operations
    operations = [
        ("create", f"random_key_{random.randint(1, 100)}", f"RandomValue_{random.randint(1, 100)}"),
        ("read", f"random_key_{random.randint(1, 100)}"),
        ("update", f"random_key_{random.randint(1, 100)}", f"UpdatedValue_{random.randint(1, 100)}"),
        ("delete", f"random_key_{random.randint(1, 100)}")
    ]

    # Randomly choose an operation
    operation = random.choice(operations)

    redis_client = None
    timestamp_received = str(time.time_ns() // 1_000_000)

    try:
        if operation[0] == "read":
            redis_host = f"{service_name}-service"
            redis_client = redis.Redis(host=redis_host, port=6379, password="Redis")
            _, key = operation
            value = await redis_client.get(key)
            print(f"Read: {key} -> {value}")
        else:
            redis_host = f"{service_name}-master-0.{service_name}-headless-service.{namespace}.svc.cluster.local"
            redis_client = redis.Redis(host=redis_host, port=6379, password="Redis")
            if operation[0] == "create":
                _, key, value = operation
                await redis_client.set(key, value)
                print(f"Created: {key} -> {value}")

            elif operation[0] == "update":
                _, key, value = operation
                await redis_client.set(key, value)
                print(f"Updated: {key} -> {value}")

            elif operation[0] == "delete":
                _, key = operation
                await redis_client.delete(key)
                print(f"Deleted: {key}")

        # Log data to the logging service
        json_data.update({
            "dm": service_name,
            "timestamp_received": timestamp_received,
            "query": str(operation),
            "status": "success"
        })
        await make_http_call_to_logging_server(json_data)

    except Exception as e:
        # Log data to the logging service
        json_data.update({
            "dm": service_name,
            "timestamp_received": timestamp_received,
            "query": str(operation),
            "status": "failure"
        })
        await make_http_call_to_logging_server(json_data)
        print(f"Error during {operation[0]} operation: {e}")

    finally:
        if redis_client:
            await redis_client.close()

import time
import psycopg2
import random
import os
from ..http.http_client import make_http_call_to_logging_server

container_name = os.environ.get("CONTAINER_NAME")

# Function to simulate DB calls with logging
async def simulate_db_call(service_name, json_data, namespace):

    timestamp_received = ""
    connection = ""
    # Define an array of SQL queries
    queries = [
        "SELECT * FROM random_data;",
        "INSERT INTO random_data (data) VALUES ('NewRandomData');",
        "DELETE FROM random_data WHERE id = (SELECT MIN(id) FROM random_data);",
        "UPDATE random_data SET data = 'UpdatedData' WHERE id = (SELECT MAX(id) FROM random_data);",
        "SELECT COUNT(*) FROM random_data;"
    ]
    # Randomly choose a query to execute
    random_query = random.choice(queries)
    try:

        if "SELECT" in random_query.split()[0]:
            # Connect to the PostgreSQL service
            connection = psycopg2.connect(
                user="user",
                password="password",
                host=f"{service_name}-service",  # This is the PostgreSQL service name
                port="5432",
                database="mydatabase"
            )
            cursor = connection.cursor()

            cursor.execute(random_query)
            result = cursor.fetchall()
            print(f"Query result: {result}")
        else:
            connection = psycopg2.connect(
                user="user",
                password="password",
                host=f"{service_name}-primary-0.{service_name}-headless-service.{namespace}.svc.cluster.local",  # This is the PostgreSQL service name
                port="5432",
                database="mydatabase"
            )
            cursor = connection.cursor()

            cursor.execute(random_query)
            connection.commit()
        timestamp_received = str(time.time_ns() // 1_000_000)
        
        # Log data to the logging service
        json_data.update({
            "dm": service_name,
            "timestamp_received": timestamp_received,
            "query": random_query,
            "status": "success"
        })
        await make_http_call_to_logging_server(json_data)

        print(f"Executed DB query for {service_name}: {random_query}")
        
    except Exception as e:
        if not timestamp_received:
            timestamp_received = str(time.time_ns() // 1_000_000)
        # Log data to the logging service
        json_data.update({
            "dm": service_name,
            "timestamp_received": timestamp_received,
            "query": random_query,
            "status": "failure"
        })
        # Log failure to the logging service
        response = await make_http_call_to_logging_server(json_data)
        print(response)
        print(f"Error during DB call: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()

import psycopg2
import random
import sys
import aiohttp
import os

container_name = os.environ.get("CONTAINER_NAME")

# Function to make a call to the logging service
async def make_db_log_call(log_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://logging-service/logs", json=log_data) as response:
                response_text = await response.text()
                print(f"Logged DB interaction: {response_text}", file=sys.stderr)
    except aiohttp.ClientError as e:
        print(f"Failed to log DB interaction: {e}", file=sys.stderr)


# Function to simulate DB calls with logging
async def simulate_db_call(service_name, json_data):
    try:
        # Connect to the PostgreSQL service
        connection = psycopg2.connect(
            user="user",
            password="password",
            host=f"{service_name}",  # This is the PostgreSQL service name
            port="5432",
            database="mydatabase"
        )
        cursor = connection.cursor()

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
        cursor.execute(random_query)
        if "SELECT" in random_query:
            result = cursor.fetchall()
            print(f"Query result: {result}")
        else:
            connection.commit()
        
        # Log data to the logging service
        log_data = {
            "um": container_name,
            "dm_service": service_name,
            "timestamp_actual": json_data['timestamp_actual'],
            "timestamp_sent": json_data['timestamp_sent'],
            "query": random_query,
            "status": "success"
        }
        await make_db_log_call(log_data)

        print(f"Executed DB query for {service_name}: {random_query}")
        
    except Exception as e:
        # Log failure to the logging service
        log_data = {
            "um": container_name,
            "dm_service": service_name,
            "timestamp_actual": json_data['timestamp_actual'],
            "timestamp_sent": json_data['timestamp_sent'],
            "query": random_query if 'random_query' in locals() else "N/A",
            "status": "failed",
            "error": str(e)
        }
        await make_db_log_call(log_data)
        print(f"Error during DB call: {e}")
    
    finally:
        if connection:
            cursor.close()
            connection.close()

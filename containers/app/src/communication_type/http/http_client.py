import sys
import aiohttp
import os

container_name = os.environ.get("CONTAINER_NAME")

async def make_http_call(data):
    try:
        json_body = {
            "timestamp_sent": data['timestamp_sent'],
            "um": container_name,
            "communication_type": data['communication_type'],
            'timestamp_actual': data['timestamp_actual']
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://{data['dm_service']}-service/", json=json_body) as response:
                response_text = await response.text()
                print(f"Contacted {data['dm_service']} with communication_type {data['communication_type']}: {response_text}", file=sys.stderr)
    except aiohttp.ClientError as e:
        print(f"Failed to contact {data['dm_service']}: {e}", file=sys.stderr)

async def make_http_call_to_logging_server(data):
    print("Entered make_http_call_to_logging_server", file=sys.stderr)
    try:
        async with aiohttp.ClientSession() as session:
            print("Before post", file=sys.stderr)
            async with session.post(f"http://logging-service/logs", json=data) as response:
                print("After post", file=sys.stderr)
                # Check if the response status indicates success
                if response.status == 200:
                    response_text = await response.text()
                    print(f"Contacted logging_capstone with communication_type {data.get('communication_type', 'unknown')}: {response_text}", file=sys.stderr)
                else:
                    # Log the status code and response if not successful
                    response_text = await response.text()
                    print(f"Failed to contact logging_capstone: Received status {response.status}, response: {response_text}", file=sys.stderr)
    except aiohttp.ClientError as e:
        print(f"Client error occurred while contacting logging_capstone: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error during HTTP call: {e}", file=sys.stderr)


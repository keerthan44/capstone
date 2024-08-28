import grpc
from .contact_grpc_pb2_grpc import LoggerServiceStub
from .contact_grpc_pb2 import ContactRequest
import argparse
import asyncio

async def contact_rpc_server(data):
    # Create an asynchronous gRPC channel
    async with grpc.aio.insecure_channel(f"{data['dm_service']}-service:50051") as channel:
        stub = LoggerServiceStub(channel)
        
        # Create a request message
        request = ContactRequest(
            um=data['um'],
            dm=data['dm_service'],
            timestamp_sent=data['timestamp_sent'],
            communication_type=data['communication_type'],
            timestamp_actual=data['timestamp_actual']
        )
        
        # Asynchronously call the gRPC method
        response = await stub.ContactServer(request)
        print(f"Server response: {response.status}")

async def main():
    parser = argparse.ArgumentParser(description='Contact gRPC server with specified parameters.')
    
    # Define default values for arguments
    parser.add_argument('--ip_address', type=str, default='localhost', help='IP address of the gRPC server')
    parser.add_argument('--um', type=str, default='default_um', help='UM parameter')
    parser.add_argument('--dm', type=str, default='default_dm', help='DM parameter')
    parser.add_argument('--timestamp', type=str, default='default_timestamp', help='Timestamp parameter')
    parser.add_argument('--communication_type', type=str, default='grpc', help='Communication type parameter')

    args = parser.parse_args()

    # Construct the data dictionary
    data = {
        'ip_address': args.ip_address,
        'um': args.um,
        'dm_service': args.dm,
        'timestamp_sent': args.timestamp,
        'communication_type': args.communication_type
    }

    await contact_rpc_server(data)

if __name__ == '__main__':
    asyncio.run(main())

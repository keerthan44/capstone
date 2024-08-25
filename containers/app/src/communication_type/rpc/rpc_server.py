from concurrent import futures
import grpc
from .contact_grpc_pb2 import ContactResponse
from .contact_grpc_pb2_grpc import LoggerServiceServicer, add_LoggerServiceServicer_to_server
from multiprocessing import Process
from ..http.flask_server import make_http_call_to_logging_server

class LoggerService(LoggerServiceServicer):
    def ContactServer(self, request, context):
        um = request.um
        dm = request.dm
        timestamp = request.timestamp
        communication_type = request.communication_type

        try:
            # Forward the data to the HTTP function
            print(f"Contacted {dm} with communication_type {communication_type} at {timestamp} from {um}")
            make_http_call_to_logging_server(um, dm, timestamp, communication_type)
            status = 'Success'
        except Exception as e:
            status = f'Failed: {str(e)}'

        return ContactResponse(status=status)

def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_LoggerServiceServicer_to_server(LoggerService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("GRPC Server started on port 50051.")
    server.wait_for_termination()

def run_grpc_server_process():
    # Create a new process to run the gRPC server
    process = Process(target=start_grpc_server)
    process.start()
    return process

if __name__ == '__main__':
    # Start the gRPC server in a separate process
    server_process = run_grpc_server_process()
    # Optionally wait for the process to finish if needed
    server_process.join()


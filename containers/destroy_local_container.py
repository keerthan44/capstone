import asyncio
import aiodocker
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

async def read_container_names(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

async def stop_and_remove_container(container_name):
    async with aiodocker.Docker() as client:
        try:
            container = await client.containers.get(container_name)
            print(f"Stopping container {container_name}...")
            await container.stop()
            print(f"Removing container {container_name}...")
            await container.delete()
        except aiodocker.exceptions.DockerError as e:
            if e.status == 404:
                print(f"Container {container_name} does not exist")
            else:
                print(f"Error with container {container_name}: {e}")

async def main():
    container_names = await read_container_names('containers.txt')
    
    with ThreadPoolExecutor(max_workers=len(container_names)) as executor:
        loop = asyncio.get_running_loop()
        
        # Using ThreadPoolExecutor to run async operations in multiple threads
        tasks = [loop.run_in_executor(executor, asyncio.ensure_future, stop_and_remove_container(name)) for name in container_names]
        
        # Adding additional containers to the tasks
        tasks.extend([
            loop.run_in_executor(executor, asyncio.ensure_future, stop_and_remove_container('redis_capstone')),
            loop.run_in_executor(executor, asyncio.ensure_future, stop_and_remove_container('logging_capstone'))
        ])
        
        await asyncio.gather(*tasks)

def main_multiprocessing():
    container_names = asyncio.run(read_container_names('containers.txt'))
    
    with Pool(processes=len(container_names)) as pool:
        pool.map(stop_and_remove_container_sync, container_names)
    
    stop_and_remove_container_sync('redis_capstone')
    stop_and_remove_container_sync('logging_capstone')

def stop_and_remove_container_sync(container_name):
    asyncio.run(stop_and_remove_container(container_name))

if __name__ == "__main__":
    main_multiprocessing()

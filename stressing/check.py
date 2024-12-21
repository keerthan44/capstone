import requests
import time

def start_cpu_stress():
    url_start = "http://10.10.3.25:5000/start"
    url_stop = "http://10.10.3.25:5000/stop"
    
    try:
        print(f"Sending POST request to {url_start}...")
        response = requests.post(url_start)
        
        if response.status_code == 200:
            print("Successfully started CPU stress.")
            print("Response:", response.json())
            
            # Wait for 60 seconds before sending the stop request
            print("Waiting for 60 seconds before stopping the CPU stress...")
            time.sleep(60)
            
            print(f"Sending POST request to {url_stop}...")
            stop_response = requests.post(url_stop)
            
            if stop_response.status_code == 200:
                print("Successfully stopped CPU stress.")
                print("Response:", stop_response.json())
            else:
                print(f"Failed to stop CPU stress. HTTP Status Code: {stop_response.status_code}")
                print("Details:", stop_response.text)
        
        else:
            print(f"Failed to start CPU stress. HTTP Status Code: {response.status_code}")
            print("Details:", response.text)
    
    except requests.exceptions.RequestException as e:
        print("An error occurred while sending the request.")
        print("Error:", str(e))

if __name__ == "__main__":
    start_cpu_stress()

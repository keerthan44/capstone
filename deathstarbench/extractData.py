import requests
import datetime
import json
import tkinter as tk
from tkinter import filedialog

# Function to fetch traces from Jaeger's API
def fetch_traces(service_name, jaeger_host='localhost', jaeger_port=16686):
    url = f"http://{jaeger_host}:{jaeger_port}/api/traces?service={service_name}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Function to read traces from a JSON file
def read_traces_from_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def convert_traces_to_custom_format(traces):
    formatted_data = []

    # Find the smallest timestamp
    min_timestamp = min(span['startTime'] for trace in traces['data'] for span in trace['spans'])

    for trace in traces['data']:
        for span in trace['spans']:
            upstream = span['references'][0]['spanID'] if span['references'] else 'root'
            downstream = span['spanID']
            timestamp = (span['startTime'] - min_timestamp) / 1e3  # Relative timestamp in milliseconds
            duration = span['duration'] / 1e3  # Convert microseconds to milliseconds
            
            formatted_data.append({
                'um': upstream,
                'dm': downstream,
                'timestamp': int(timestamp),
                'duration': duration
            })
    
    return formatted_data

# Function to print the formatted trace data
def print_formatted_traces(formatted_traces):
    for trace in formatted_traces:
        print(f"Upstream: {trace['upstream']}, Downstream: {trace['downstream']}, Timestamp: {trace['timestamp']}, Duration: {trace['duration']} ms")

# Function to save formatted traces to a JSON file
def save_traces_to_file(formatted_traces, file_path='traces.json'):
    with open(file_path, 'w') as file:
        json.dump(formatted_traces, file, indent=4)
    print(f"Formatted trace data saved to {file_path}")

# Function to open a file dialog and get the selected file path
def get_file_path():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(title="Select a trace file", filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
    return file_path

# Function to display the menu
def display_menu():
    print("Menu:")
    print("1. Fetch traces from API")
    print("2. Read traces from file")
    print("3. Exit")

# Main function
def main():
    while True:
        display_menu()
        choice = input("Enter your choice (1/2/3): ").strip()
        
        if choice == '1':
            service_name = input("Enter the service name: ").strip()
            traces = fetch_traces(service_name)
            formatted_traces = convert_traces_to_custom_format(traces)
            # print_formatted_traces(formatted_traces)
            save_traces_to_file(formatted_traces)
        elif choice == '2':
            file_path = get_file_path()
            if file_path:
                traces = read_traces_from_file(file_path)
                formatted_traces = convert_traces_to_custom_format(traces)
                # print_formatted_traces(formatted_traces)
                save_traces_to_file(formatted_traces)
            else:
                print("No file selected.")
        elif choice == '3':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please choose 1, 2, or 3.")

if __name__ == "__main__":
    main()

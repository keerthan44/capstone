import json
import pandas as pd
import os
from collections import defaultdict

current_dir = os.path.dirname(os.path.abspath(__file__))
target_dir = os.path.join(current_dir, "..", "containers")

def read_and_sort_csv(file_name):
    df = pd.read_csv(file_name)
    sorted_df = df.sort_values(by='timestamp')
    return sorted_df

def process_v2021(file_name):
    df = read_and_sort_csv(file_name)
    result = {}
    containers = set()

    for _, row in df.iterrows():
        timestamp = row['timestamp']
        um = row['um']
        dm = row['dm']
        rpctype = row['rpctype']

        if '?' in um or '?' in dm:
            continue
        containers.update([um, dm])

        if um not in result:
            result[um] = {}

        if timestamp not in result[um]:
            result[um][timestamp] = []
        result[um][timestamp].append({
            "dm_service": dm,
            "communication_type": rpctype
        })

    ms_replicas = {}
    for _, row in df.iterrows():
        msname = row['msName']
        msinstanceid = row['msinstanceid']

        if msname not in ms_replicas:
            ms_replicas[msname] = set()

        ms_replicas[msname].add(msinstanceid)

    containers_json = [
        {"msName": msname, "replicas": len(instances)}
        for msname, instances in ms_replicas.items()
    ]
    
    calls_output_file = os.path.join(target_dir, "calls.json")
    with open(calls_output_file, 'w') as f:
        json.dump(result, f, indent=4)

    containers_output_file = os.path.join(target_dir, "containers.json")
    with open(containers_output_file, 'w') as f:
        json.dump(containers_json, f, indent=4)

    print("Processing complete. 'calls.json' and 'containers.json' have been written.")

def process_v2022(file_name):
    df = read_and_sort_csv(file_name)
    result = {}
    service_replicas = {}

    for _, row in df.iterrows():
        timestamp = row['timestamp']
        um = row['um']
        dm = row['dm']
        uminstanceid = row['uminstanceid']
        dminstanceid = row['dminstanceid']
        rpctype = row['rpctype']

        if '?' in um or '?' in dm:
            continue

        if um not in service_replicas:
            service_replicas[um] = set()
        service_replicas[um].add(uminstanceid)

        if dm not in service_replicas:
            service_replicas[dm] = set()
        service_replicas[dm].add(dminstanceid)

        if um not in result:
            result[um] = {}
        if timestamp not in result[um]:
            result[um][timestamp] = []
        result[um][timestamp].append({
            "dm_service": dm,
            "communication_type": rpctype
        })

    containers_json = [
        {"msName": service, "replicas": len(instances)}
        for service, instances in service_replicas.items()
    ]
    
    calls_output_file = os.path.join(target_dir, "calls.json")
    with open(calls_output_file, 'w') as f:
        json.dump(result, f, indent=4)

    containers_output_file = os.path.join(target_dir, "containers.json")
    with open(containers_output_file, 'w') as f:
        json.dump(containers_json, f, indent=4)

    print("Processing complete. 'calls.json' and 'containers.json' have been written.")

def process_probabilities(file_name):
    df = read_and_sort_csv(file_name)

    probability_results = {}
    communication_counter = defaultdict(int)

    for _, row in df.iterrows():
        communication_type = row['rpctype']
        communication_counter[communication_type] += 1

    total_calls = sum(communication_counter.values())
    probabilities = {comm_type: round(count / total_calls, 3) for comm_type, count in communication_counter.items()}

    probability_results['probabilities'] = probabilities

    probabilities_output_file = os.path.join(target_dir, "probabilities.json")
    with open(probabilities, 'w') as f:
        json.dump(probability_results, f, indent=4)

    print("Processing complete. 'probabilities.json' has been written.")

def main():
    print("Please select the mode for processing:")
    print("1. v2021")
    print("2. v2022")
    print("3. Probabilities")
    mode = input("Enter the mode number: ").strip()

    file = input("Enter the name of the CSV file (e.g., data.csv): ").strip()

    if mode == '1':
        process_v2021(file)
    elif mode == '2':
        process_v2022(file)
    elif mode == '3':
        process_probabilities(file)
    else:
        print("Invalid mode selected.")

if __name__ == '__main__':
    main()

import json

def convertJSON(data):
    containers = []
    result = {}
    for row in data:  
        timestamp = row['timestamp']
        um = row['um']
        dm = row['dm']
        rpctype = 'http'

        if '?' in um or '?' in dm:
            continue
        if um not in containers:
            containers.append(um)
        if dm not in containers:
            containers.append(dm)

        if um not in result:
            result[um] = {}

        if timestamp not in result[um]:
            result[um][timestamp] = []
        result[um][timestamp].append({
            "dm_service": dm,
            "communication_type": rpctype
        })
    
    with open('../containers/calls.json', 'w') as f:
        json.dump(result, f)
    # containers.append(containers.pop(0))
    with open('../containers/containers.txt', 'w') as f:
        f.writelines(f"{container}\n" for container in containers)

def read_json_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            print(json.dumps(data, indent=4))  # Pretty-print the JSON data
            return data
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from the file: {file_path}")

def main():
    data = read_json_from_file('traces.json')
    convertJSON(data)

if __name__ == "__main__":
    main()


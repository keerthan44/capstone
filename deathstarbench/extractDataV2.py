import json

# Load the JSON data from the uploaded file
file_path = './data/deathstarbench.json'
output_calls_path = 'traces.json'

def extract_data(file_path, output_calls_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Extract the global minimum timestamp
    min_timestamp = float('inf')
    for trace in data['data']:
        for span in trace['spans']:
            for ref in span['references']:
                if ref['refType'] == 'CHILD_OF':
                    for parent_span in trace['spans']:
                        if parent_span['spanID'] == ref['spanID']:
                            if span['startTime'] < min_timestamp:
                                min_timestamp = span['startTime']
    print(min_timestamp) 
    # Extract calls and normalize timestamps
    calls = []
    for trace in data['data']:
        for span in trace['spans']:
            upstream_service = trace['processes'][span['processID']]['serviceName']
            for ref in span['references']:
                if ref['refType'] == 'CHILD_OF':
                    for parent_span in trace['spans']:
                        if parent_span['spanID'] == ref['spanID']:
                            downstream_service = trace['processes'][parent_span['processID']]['serviceName']
                            print(span['startTime'], min_timestamp, span['startTime'] - min_timestamp)
                            call = {
                                "um": upstream_service,
                                "dm": downstream_service,
                                "timestamp": span['startTime'] - min_timestamp,
                                "duration": span['duration']
                            }
                            calls.append(call)

    # Write calls to calls.json
    with open(output_calls_path, 'w') as file:
        json.dump(calls, file, indent=4)

extract_data(file_path, output_calls_path)
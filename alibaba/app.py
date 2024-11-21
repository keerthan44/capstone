from flask import Flask, request, jsonify, render_template , redirect , url_for
import json
import pandas as pd
import os
from collections import defaultdict


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/v2022')
def v2022_form():
    return render_template('v2022.html')

@app.route('/probabilities_form')
def probabilities_form():
    return render_template('probabilities.html')

@app.route('/upload', methods=['POST'])
def upload():
    file1 = request.files['file1']
    file2 = request.files['file2']

    if not file1 or not file2:
        return "Both files are required"

    def read_and_sort_csv(file):
        df = pd.read_csv(file)
        sorted_df = df.sort_values(by='timestamp')
        return sorted_df

    # Process the first CSV to generate calls.json
    df1 = read_and_sort_csv(file1)
    result = {}
    containers = set()

    for _, row in df1.iterrows():
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

    # Process the second CSV to generate containers.json
    df2 = pd.read_csv(file2)
    ms_replicas = {}

    for _, row in df2.iterrows():
        msname = row['msName']
        msinstanceid = row['msinstanceid']

        if msname not in ms_replicas:
            ms_replicas[msname] = set()

        ms_replicas[msname].add(msinstanceid)

    # Debugging: Print to console to verify content
    print("ms_replicas:", ms_replicas)

    containers_json = [
        {"msName": msname, "replicas": len(instances)}
        for msname, instances in ms_replicas.items()
    ]

    # Debugging: Print to console to verify content
    print("containers_json:", containers_json)

    # output_directory = os.path.join(os.getcwd(), 'containers')
    # os.makedirs(output_directory, exist_ok=True)

    # Write calls.json
    with open('../containers/calls.json', 'w') as f:
        json.dump(result, f, indent=4)

    # Write containers.json
    with open('../containers/containers.json', 'w') as f:
        json.dump(containers_json, f, indent=4)

    # Debugging: Confirm that the file was written
    
    return jsonify({
        "calls": result,
        "containers": containers_json
    })

@app.route('/v2022', methods=['POST'])
def v2022():
    file = request.files['file']

    if not file:
        return "File is required"

    def read_and_sort_csv(file):
        df = pd.read_csv(file)
        sorted_df = df.sort_values(by='timestamp')
        return sorted_df

    # Process the CSV to generate the required output
    df = read_and_sort_csv(file)
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

        # Update replicas for um
        if um not in service_replicas:
            service_replicas[um] = set()
        service_replicas[um].add(uminstanceid)

        # Update replicas for dm
        if dm not in service_replicas:
            service_replicas[dm] = set()
        service_replicas[dm].add(dminstanceid)

        # Build the call graph
        if um not in result:
            result[um] = {}
        if timestamp not in result[um]:
            result[um][timestamp] = []
        result[um][timestamp].append({
            "dm_service": dm,
            "communication_type": rpctype
        })

    # Generate containers.json
    containers_json = [
        {"msName": service, "replicas": len(instances)}
        for service, instances in service_replicas.items()
    ]

    # Write calls.json
    with open('../containers/calls.json', 'w') as f:
        json.dump(result, f, indent=4)

    # Write containers.json
    with open('../containers/containers.json', 'w') as f:
        json.dump(containers_json, f, indent=4)

    return jsonify({
        "calls": result,
        "containers": containers_json
    })

@app.route('/probabilities', methods=['GET', 'POST'])
def probabilities():
    if request.method == 'GET':
        return redirect(url_for('probabilities_form'))
    
    file = request.files.get('file')
    if not file:
        return "JSON file is required", 400
    
    data = json.load(file)
    probability_results = {}

    for service, timestamps in data.items():
        communication_counter = defaultdict(int)

        for interactions in timestamps.values():
            for interaction in interactions:
                communication_type = interaction['communication_type']
                communication_counter[communication_type] += 1

        total_calls = sum(communication_counter.values())
        probabilities = {comm_type: round(count / total_calls, 3) for comm_type, count in communication_counter.items()}
        
        probability_results[service] = probabilities

    with open('../containers/probabilities.json', 'w') as f:
        json.dump(probability_results, f, indent=4)

    return jsonify(probability_results)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=3000)

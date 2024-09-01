from flask import Flask, request, jsonify, render_template
import json
import pandas as pd
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/v2022')
def v2022_form():
    return render_template('v2022.html')

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
    service_replicas = {}

    for _, row in df1.iterrows():
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

    # Process the second CSV (not needed in the current logic but kept for completeness)
    df2 = pd.read_csv(file2)
    # No processing needed as we're using instance IDs from the first file

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

if __name__ == '__main__':
    app.run(debug=True, port=3000)

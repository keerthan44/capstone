from flask import Flask, request, jsonify, render_template
import json
import pandas as pd
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

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

if __name__ == '__main__':
    app.run(debug=True, port=3000)
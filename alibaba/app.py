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
    file = request.files['file']
    if not file:
        return "No file"

    def read_and_sort_csv(file_path):
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Sort the dataframe by the 'timestamp' column
        sorted_df = df.sort_values(by='timestamp')

        return sorted_df

    df = read_and_sort_csv(file)

    result = {}
    containers = []

    for index, row in df.iterrows():
        timestamp = row['timestamp']
        um = row['um']
        dm = row['dm']
        rpctype = row['rpctype']

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

    os.makedirs('../containers', exist_ok=True)
    with open('../containers/calls.json', 'w') as f:
        json.dump(result, f, indent=4)

    with open('../containers/containers.txt', 'w') as f:
        f.writelines(f"{container}\n" for container in containers)

    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True, port=3000)

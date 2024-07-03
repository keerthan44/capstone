from flask import Flask, request, jsonify, render_template
import pandas as pd

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    if not file:
        return "No file"
    df = pd.read_csv(file)



    result = {}


    for index, row in df.iterrows():
        timestamp = row['timestamp']
        um = row['um']
        dm = row['dm']


        if '?' in um or '?' in dm:
            continue


        if timestamp not in result:
            result[timestamp] = {}


        if um not in result[timestamp]:
            result[timestamp][um] = []


        result[timestamp][um].append(dm)

    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)




def process_data(file_path):

    df = pd.read_csv(file_path)    
    return result

# Example usage
file_path = '/Users/premsemitha/capstone/trace_0_data_modified_with_suffixes.csv'
output_dict = process_data(file_path)
print(output_dict)
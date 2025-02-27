import json

# Read the JSON file
with open('containers.json', 'r') as file:
    data = json.load(file)

# Sum up the replicas
total_replicas = sum(item['replicas'] for item in data)

# Print the total
print("Total Replicas:", total_replicas)
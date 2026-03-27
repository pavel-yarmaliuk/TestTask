import json
import logging

def load_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data.get("data", [])
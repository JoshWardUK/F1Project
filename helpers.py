import os
import shutil
import json 

def get_total_from_json(data):
    """
    Extracts the 'total' value from the given JSON.

    Args:
        data (dict): The JSON data as a Python dictionary.

    Returns:
        str: The 'total' value from the JSON.
    """
    return data.get("MRData", {}).get("total", None)

def cleanup():

    dir_path = "./landing_zone/"
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        print(f"Removed directory: {dir_path}")
    else:
        print("Directory does not exist.")

def cleanup_streamlit():

    dir_path = "./landing_zone/streamlit/"
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        print(f"Removed directory: {dir_path}")
    else:
        print("Directory does not exist.")

def load_function_checkpoint(path='checkpoints/function_checkpoint.json'):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}

def save_function_checkpoint(fn_name, path='checkpoints/function_checkpoint.json'):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    checkpoint = load_function_checkpoint(path)
    checkpoint[fn_name] = True
    with open(path, 'w') as f:
        json.dump(checkpoint, f)

def save_function_checkpoint_season(key, value, path='checkpoints/function_checkpoint.json'):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    checkpoint = load_function_checkpoint(path)
    checkpoint[key] = value
    with open(path, 'w') as f:
        json.dump(checkpoint, f)

def clear_directory(path):
    if os.path.exists(path) and os.path.isdir(path):
        print(f"Clearing partial data in: {path}")
        shutil.rmtree(path)
    else:
        print(f"Directory not found: {path}. Nothing to clear.")
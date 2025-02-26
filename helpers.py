import os
import shutil

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
def get_total_from_json(data):
    """
    Extracts the 'total' value from the given JSON.

    Args:
        data (dict): The JSON data as a Python dictionary.

    Returns:
        str: The 'total' value from the JSON.
    """
    return data.get("MRData", {}).get("total", None)
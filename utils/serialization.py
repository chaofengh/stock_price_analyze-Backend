import numpy as np
import pandas as pd

def convert_to_python_types(obj):
    """
    Recursively convert NumPy/Pandas objects to native Python types.
    """
    if isinstance(obj, dict):
        return {k: convert_to_python_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_python_types(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, pd.Timestamp):
        # Convert Timestamps to string (ISO 8601 format, for example)
        return obj.isoformat()
    else:
        return obj

import json
from pathlib import Path

def load_config(path: str = "config.json") -> dict:
    """
    Load configuration from a JSON file.

    Args:
        path (str): Path to the config file. Defaults to 'config.json'.

    Returns:
        dict: Configuration as a Python dictionary.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
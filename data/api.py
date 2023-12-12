import cfbd
from os import path

_KEY_FILE = path.join(path.dirname(__file__), "key")

def create_api_client() -> cfbd.ApiClient:
    config = cfbd.Configuration()

    with open(_KEY_FILE, "r") as f:
        config.api_key["Authorization"] = f.read().strip()
        config.api_key_prefix["Authorization"] = "Bearer"

    return cfbd.ApiClient(config)


def create_drive_client() -> cfbd.DrivesApi:
    client = create_api_client()
    return cfbd.DrivesApi(client)


def create_play_client() -> cfbd.PlaysApi:
    client = create_api_client()
    return cfbd.PlaysApi(client)


def create_teams_client() -> cfbd.TeamsApi:
    client = create_api_client()
    return cfbd.TeamsApi(client)

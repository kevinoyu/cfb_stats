import cfbd
from os import path

from api import create_api_client
from typing import NamedTuple

import pandas as pd

TEAMS_DIR = "teams"
TEAM_FILE = "team.pq"
TEAM_FILE_PATH = path.join(TEAMS_DIR, TEAM_FILE)


# transform swagger-returned team into internal team repr, stripping unnecessary fields for compactness
def transform_teams(teams: list[cfbd.Team]) -> pd.DataFrame:
    CATEGORICAL = ["classification", "conference", "division"]

    team_gen = (
        {
            "name": team.school,
            "abbr": team.abbreviation,
            "classification": team.classification,
            "conference": team.conference,
            "division": team.division,
        }
        for team in teams
    )
    id_gen = (team.id for team in teams)

    df = pd.DataFrame(team_gen, index=id_gen)
    df[CATEGORICAL] = df[CATEGORICAL].astype("category")

    return df


def read_teams() -> pd.DataFrame:
    return pd.read_parquet(TEAM_FILE_PATH)


def store_teams(teams: pd.DataFrame) -> None:
    teams.to_parquet(TEAM_FILE_PATH, compression="snappy")


def _fetch_from_api() -> pd.DataFrame:
    client = create_api_client()
    team_client = cfbd.TeamsApi(client)
    teams = team_client.get_teams()
    df = transform_teams(teams)
    store_teams(df)
    return df

def _try_read() -> pd.DataFrame:
    try:
        return read_teams()
    except FileNotFoundError:
        print("File not found - pulling from API")
        return _fetch_from_api()

def get_teams(use_cache: bool = True) -> pd.DataFrame:
    if use_cache:
        df = _try_read()
    else:
        df = _fetch_from_api()

    return df


if __name__ == "__main__":
    teams = get_teams()
    print(teams)

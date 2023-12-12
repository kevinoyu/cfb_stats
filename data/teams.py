import cfbd
from os import path

from api import create_teams_client
from util import read_file, write_file
from typing import NamedTuple

import pandas as pd

TEAM_FILE_PATH = path.join(path.dirname(__file__), "teams", "team.pq")


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


def _fetch_from_api() -> pd.DataFrame:
    client = create_teams_client()
    teams = client.get_teams()
    df = transform_teams(teams)
    write_file(df, TEAM_FILE_PATH)
    return df


def _try_read() -> pd.DataFrame:
    try:
        return read_file(TEAM_FILE_PATH)
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

import pandas as pd
from os import path
from collections.abc import Iterable
import cfbd

from api import create_api_client
from util import game_time_to_timestamp, read_file, write_file

PLAY_DIR = "plays"


def get_file_name(year: int = 2023) -> str:
    return path.join(PLAY_DIR, f"{year}.pq")


def transform_plays(plays: list[cfbd.Play]) -> pd.DataFrame:
    CATEGORICAL = ["offense", "defense", "type", "quarter", "down"]
    STRING = ["description"]
    STRING = ["scoring"]
    DROP = ["mins", "secs", "home"]

    play_gen = (
        {
            "game": play.game_id,
            "drive": play.drive_id,
            "drive_number": play.drive_number,
            "play_number": play.play_number,
            "offense": play.offense,
            "defense": play.defense,
            "down": play.down,
            "distance": play.distance,
            "yards_to_goal": play.yards_to_goal,
            "yards_gained": play.yards_gained,
            "type": play.play_type,
            "offense_timeouts": play.offense_timeouts,
            "defense_timeouts": play.defense_timeouts,
            "quarter": play.period,
            "description": play.play_text,
            "scoring": play.scoring,
            "ppa": play.ppa,
            "mins": play.clock.minutes,
            "secs": play.clock.seconds,
            "home": play.home,
        }
        for play in plays
    )
    id_gen = (play.id for play in plays)

    df = pd.DataFrame(play_gen, index=id_gen)

    df["time"] = game_time_to_timestamp(df["quarter"], df["mins"], df["secs"])
    df["is_home_offense"] = df["offense"] == df["home"]

    df.drop(DROP, inplace=True, axis=1)

    df[CATEGORICAL] = df[CATEGORICAL].astype("category")

    return df


def _try_read(year: int) -> pd.DataFrame:
    try:
        return read_file(year)
    except FileNotFoundError:
        return _fetch_from_api(year)


def _fetch_from_api(year: int) -> pd.DataFrame:
    client = create_api_client()
    play_client = cfbd.PlaysApi(client)
    plays = play_client.get_plays(year, classification="fbs")

    data = transform_plays(plays)

    file = get_file_name(year)
    write_file(data, file)

    return data


def get_drives(years: Iterable[int], use_cache: bool = True) -> pd.DataFrame:
    years = [2023] if years is None else years

    if use_cache:
        data = pd.concat(_try_read(year) for year in years)
    else:
        data = pd.concat((_fetch_from_api(year) for year in years))

    return data


if __name__ == "__main__":
    drives = get_drives()

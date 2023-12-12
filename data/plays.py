import pandas as pd
from os import path
from collections.abc import Iterable
import cfbd
import argparse

from .api import create_play_client
from .util import game_time_to_timestamp, read_file, write_file

PLAY_DIR = path.join(path.dirname(__file__), "plays")

WEEKS = range(1, 14)


def get_file_name(year: int, week: int) -> str:
    return path.join(PLAY_DIR, f"{year}_{week}.pq")


def transform_plays(plays: list[cfbd.Play]) -> pd.DataFrame:
    CATEGORICAL = ["offense", "defense", "type"]
    UINT8 = [
        "drive_number",
        "play_number",
        "offense_timeouts",
        "defense_timeouts",
        "quarter",
        "down",
        "yards_gained",
        "yards_to_goal",
    ]
    STRING = ["description"]
    DROP = ["mins", "secs", "home"]

    IGNORE_TYPES = [
        "End Period",
        "Timeout",
        "End of Half",
        "End of Game",
        "End of Regulation",
    ]

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

    df[CATEGORICAL] = df[CATEGORICAL].astype("category")

    df["time"] = game_time_to_timestamp(df["quarter"], df["mins"], df["secs"])
    df["is_home_offense"] = df["offense"] == df["home"]
    df.loc[df["offense_timeouts"] < 0, "offense_timeouts"] += 4
    df.loc[df["defense_timeouts"] < 0, "defense_timeouts"] += 4

    df.drop(DROP, inplace=True, axis=1)

    df[CATEGORICAL] = df[CATEGORICAL].astype("category", copy=False, errors="ignore")
    df[UINT8] = df[UINT8].astype("uint8", copy=False, errors="ignore")
    df[STRING] = df[STRING].astype("string", copy=False, errors="ignore")

    return df[~df["type"].isin(IGNORE_TYPES)]


def _try_read(year: int, week: int) -> pd.DataFrame:
    try:
        file = get_file_name(year, week)
        return read_file(file)
    except FileNotFoundError:
        return _fetch_from_api(year, week)


def _fetch_from_api(year: int, week: int) -> pd.DataFrame:
    client = create_play_client()
    plays = client.get_plays(year, week, classification="fbs")

    data = transform_plays(plays)

    file = get_file_name(year, week)
    write_file(data, file)

    return data


def get_plays(years: Iterable[int] = None, use_cache: bool = True) -> pd.DataFrame:
    years = [2023] if years is None else years

    if use_cache:
        data = pd.concat(_try_read(year, week) for year in years for week in WEEKS)
    else:
        data = pd.concat(
            (_fetch_from_api(year, week) for year in years for week in WEEKS)
        )

    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("years", nargs="+", type=int)
    args = parser.parse_args()

    get_plays(years=args.years)

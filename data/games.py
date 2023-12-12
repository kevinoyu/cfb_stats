import pandas as pd
from os import path
from collections.abc import Iterable
import cfbd
import argparse

from .api import create_drive_client
from .util import game_time_to_timestamp, read_file, write_file

DRIVE_DIR = path.join(path.dirname(__file__), "plays")


def get_file_name(year: int = 2023) -> str:
    return path.join(DRIVE_DIR, f"{year}.pq")


def transform_drives(drives: list[cfbd.Drive]) -> pd.DataFrame:
    CATEGORICAL = ["offense", "defense", "result_str"]
    UINT8 = ["start_quarter", "end_quarter", "start_yardline", "end_yardline"]
    DROP = ["start_minutes", "start_seconds", "end_minutes", "end_seconds"]

    drive_gen = (
        {
            "game": drive.game_id,
            "offense": drive.offense,
            "defense": drive.defense,
            "start_yardline": drive.start_yardline,
            "end_yardline": drive.end_yardline,
            "number": drive.drive_number,
            "result_str": drive.drive_result,
            "result": drive.end_offense_score
            - drive.start_offense_score
            + drive.start_defense_score
            - drive.end_defense_score,
            "start_quarter": drive.start_period,
            "end_quarter": drive.end_period,
            "start_minutes": drive.start_time.minutes,
            "start_seconds": drive.start_time.seconds,
            "end_minutes": drive.end_time.minutes,
            "end_seconds": drive.end_time.seconds,
            "is_home_offense": drive.is_home_offense,
        }
        for drive in drives
    )
    id_gen = (drive.id for drive in drives)

    df = pd.DataFrame(drive_gen, index=id_gen)

    df["start_time"] = game_time_to_timestamp(
        df["start_quarter"], df["start_minutes"], df["start_seconds"]
    )

    df["end_time"] = game_time_to_timestamp(
        df["end_quarter"], df["end_minutes"], df["end_seconds"]
    )

    df["duration"] = df["end_time"] - df["start_time"]
    df[CATEGORICAL] = df[CATEGORICAL].astype("category", copy=False, errors="ignore")
    df[UINT8] = df[UINT8].astype("uint8", copy=False, errors="ignore")

    df.drop(DROP, inplace=True, axis=1)

    return df


def _try_read(year: int) -> pd.DataFrame:
    try:
        file = get_file_name(year)
        return read_file(file)
    except FileNotFoundError:
        return _fetch_from_api(year)


def _fetch_from_api(year: int) -> pd.DataFrame:
    client = create_drive_client()
    drives = client.get_drives(year, classification="fbs")

    data = transform_drives(drives)

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
    parser = argparse.ArgumentParser()
    parser.add_argument("years", nargs="+")
    args = parser.parse_args()

    get_drives(years=args.years)

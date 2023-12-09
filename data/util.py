import pandas as pd

_MINUTES_PER_QUARTER = 15
_SECONDS_PER_MINUTE = 60
_SECONDS_PER_QUARTER = _MINUTES_PER_QUARTER * _SECONDS_PER_MINUTE


def game_time_to_timestamp(quarter: any, mins: any, secs: any) -> any:
    return quarter * _SECONDS_PER_QUARTER - mins * _SECONDS_PER_MINUTE - secs


def read_file(file: str) -> pd.DataFrame:
    return pd.read_parquet(file)


def write_file(data: pd.DataFrame, file: str) -> None:
    data.to_parquet(file, compression="snappy")


from typing import Generator

import pandas as pd
from os import path

from .fetcher import DataFetcher
from .api import create_drive_client

from .util import game_time_to_timestamp


class DriveFetcher(DataFetcher):
    CAST = {
        "category": ["offense", "defense", "result_str"],
        "uint8": [
            "start_quarter",
            "end_quarter",
            "start_yardline",
            "end_yardline",
            "yards",
            "plays",
            "yards_to_goal",
            "number",
        ],
    }

    DROP = ["start_minutes", "start_seconds", "end_minutes", "end_seconds"]

    def __init__(self, dir=None, use_cache=True):
        dir = path.join(path.dirname(__file__), "drives") if dir is None else dir

        super().__init__(dir, use_cache)
        self.client = create_drive_client()

    def _create_data_generator(self, data: list) -> Generator:
        return (
            {
                "game": drive.game_id,
                "offense": drive.offense,
                "defense": drive.defense,
                "start_yardline": drive.start_yardline,
                "end_yardline": drive.end_yardline,
                "yards_to_goal": drive.start_yards_to_goal,
                "number": drive.drive_number,
                "plays": drive.plays,
                "yards": drive.yards,
                "scoring": drive.scoring,
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
            for drive in data
        )

    def _create_index_generator(self, data: list) -> Generator:
        return (drive.id for drive in data)

    def _transform(self, data: pd.DataFrame) -> None:
        data["start_time"] = game_time_to_timestamp(
            data["start_quarter"], data["start_minutes"], data["start_seconds"]
        )

        data["end_time"] = game_time_to_timestamp(
            data["end_quarter"], data["end_minutes"], data["end_seconds"]
        )

        data["duration"] = data["end_time"] - data["start_time"]
        data.drop(self.DROP, inplace=True, axis=1)

    def _api_call(self, year: int, week: int) -> list:
        return self.client.get_drives(year=year, week=week, classification="fbs")

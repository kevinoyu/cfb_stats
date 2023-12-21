from os import path
from typing import Generator
import pandas as pd

from .util import game_time_to_timestamp
from .api import create_play_client
from .fetcher import DataFetcher


class PlayFetcher(DataFetcher):
    CAST = {
        "category": ["offense", "defense", "type"],
        "uint8": [
            "drive_number",
            "play_number",
            "offense_timeouts",
            "defense_timeouts",
            "quarter",
            "down",
            "yards_gained",
            "yards_to_goal",
        ],
        "str": ["description"],
    }

    DROP = ["mins", "secs", "home"]

    def __init__(self, dir: str = None, use_cache: bool = True):
        dir = path.join(path.dirname(__file__), "plays") if dir is None else dir
        super().__init__(dir, use_cache)

        self.client = create_play_client()

    def _transform(self, data: pd.DataFrame) -> None:
        data["time"] = game_time_to_timestamp(
            data["quarter"], data["mins"], data["secs"]
        )
        data["is_home_offense"] = data["offense"] == data["home"]
        data.loc[data["offense_timeouts"] < 0, "offense_timeouts"] += 4
        data.loc[data["defense_timeouts"] < 0, "defense_timeouts"] += 4
        data.drop(self.DROP, inplace=True, axis=1)

    def _create_data_generator(self, data: list) -> Generator:
        return (
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
            for play in data
        )

    def _create_index_generator(self, data: list) -> Generator:
        return (play.id for play in data)

    def _api_call(self, year: int, week: int) -> list:
        return self.client.get_plays(year=year, week=week, classification="fbs")

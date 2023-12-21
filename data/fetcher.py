from .api import create_play_client
from .util import game_time_to_timestamp, read_file, write_file

from abc import ABC, abstractmethod
import pandas as pd
from os import path

from typing import Generator


class DataFetcher(ABC):
    WEEKS = range(1, 14)
    CAST = {}

    def __init__(self, dir: str, use_cache: bool = True):
        self.dir = dir
        self.use_cache = use_cache

    def _get_file_name(self, year: int, week: int) -> str:
        return path.join(self.dir, f"{year}_{week}.pq")

    def get_data(self, years: list[int] = None) -> pd.DataFrame:
        years = [2023] if years is None else years

        if self.use_cache:
            data = pd.concat(
                self._try_read(year, week) for year in years for week in self.WEEKS
            )
        else:
            data = pd.concat(
                (
                    self._fetch_from_api(year, week)
                    for year in years
                    for week in self.WEEKS
                )
            )

        return data

    def _try_read(self, year: int, week: int) -> pd.DataFrame:
        file = self._get_file_name(year, week)
        try:
            return read_file(file)
        except FileNotFoundError:
            data = self._fetch_from_api(year, week)
            write_file(data, file)
            return data

    def _fetch_from_api(self, year: int, week: int) -> pd.DataFrame:
        data = self._api_call(year, week)
        df = self._to_dataframe(data)

        self._transform(df)
        self._cast(df)

        return df

    def _to_dataframe(self, data: list) -> pd.DataFrame:
        data_gen = self._create_data_generator(data)
        index_gen = self._create_index_generator(data)

        return pd.DataFrame(data_gen, index=index_gen)

    def _cast(self, data: pd.DataFrame) -> None:
        for type, cols in self.CAST.items():
            data[cols] = data[cols].astype(type, copy=False, errors="ignore")

    @abstractmethod
    def _transform(self, data: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def _create_data_generator(self, data: list) -> Generator:
        pass

    @abstractmethod
    def _create_index_generator(self, data: list) -> Generator:
        pass

    @abstractmethod
    def _api_call(self, year: int, week: int) -> list:
        pass

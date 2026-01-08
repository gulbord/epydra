import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import override

import polars as pl
from polars.exceptions import OutOfBoundsError

from epydra.io import Reader
from epydra.types import Resolution


class Cleaner(ABC):
    reader: Reader

    @abstractmethod
    def read(self) -> pl.DataFrame:
        pass


class AutomaticCleaner(Cleaner):
    reader: Reader
    resolution: Resolution

    def __init__(self, reader: Reader) -> None:
        self.reader = reader
        self.resolution = _resolution_from_path(reader.path)

    def _column_names(self, data: pl.DataFrame) -> list[str]:
        try:
            header: tuple[str, ...] = data.row(2)
        except OutOfBoundsError:
            raise ValueError("header row not found")
        names = [re.sub(r"\s+", "", x) if x else x for x in header]
        return [x if x else str(i) for i, x in enumerate(names)]

    def _filter_dates(self, data: pl.DataFrame) -> pl.DataFrame:
        return data.filter(
            pl.first().is_not_null(),
            ~pl.first().str.contains("Val", literal=True),
        )

    @override
    def read(self) -> pl.DataFrame:
        try:
            data = self.reader.read()
            data.columns = self._column_names(data)
        except (OSError, ValueError):
            return pl.DataFrame()

        return self._filter_dates(data)



def _resolution_from_path(path: Path) -> Resolution:
    if (resolution := path.name[0].upper()) not in ("h", "d"):
        raise ValueError("malformed path, not starting with H/D")
    return resolution

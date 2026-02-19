from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, override

import polars as pl
import polars.selectors as cs

from epydra.types import (
    DATE_COLUMN,
    HOUR_COLUMN,
    FilenamePrefixError,
    SiravCodeError,
)


class Reader(ABC):
    path: Path
    sirav_code: int

    def __init__(self, path: Path) -> None:
        self.path = _validate_path(path)
        self.sirav_code = _get_sirav_code(self.path)

    @abstractmethod
    def read(self) -> pl.DataFrame: ...


class CSVReader(Reader):
    @override
    def read(self) -> pl.DataFrame:
        try:
            return pl.read_csv(
                self.path,
                encoding="latin-1",
                has_header=False,
                infer_schema_length=3,
                truncate_ragged_lines=True,
            )
        except pl.exceptions.NoDataError:
            return pl.DataFrame()


def _validate_path(path: Path) -> Path:
    path = path.expanduser().resolve()
    filename = path.name
    if filename[0] not in ("H", "D", "M"):
        raise FilenamePrefixError(filename)
    return path


def _get_sirav_code(path: Path) -> int:
    filename = path.name
    try:
        return int(filename[7:13])
    except ValueError:
        raise SiravCodeError(filename)


def make_reader(path: Path) -> Reader:
    if path.suffix != ".csv":
        raise NotImplementedError(
            f"file extension '{path.suffix}' is not supported"
        )
    return CSVReader(path)


def write_dataframe(
    data: pl.DataFrame,
    path: Path,
    *,
    format: Literal["csv", "xlsx"],
    verbose: bool = False,
) -> Path | None:
    if data.is_empty():
        if verbose:
            print(f"Skipping empty file {path}")
        return None

    path = _validate_path(path).with_suffix("." + format)
    if verbose:
        print(f"Writing {path}")

    if format == "csv":
        data.write_csv(path)
    else:
        _ = data.map_columns(  # pyright: ignore[reportCallIssue]
            cs.exclude(DATE_COLUMN, HOUR_COLUMN),
            lambda s: (
                s.cast(pl.Float64)
                if s.str.contains(".", literal=True).any()
                else s.cast(pl.Int64)
            ),
        ).write_excel(
            path, autofit=True, dtype_formats={pl.Float64: "0.######"}
        )
    return path

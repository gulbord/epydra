import re
from abc import ABC, abstractmethod
from os import PathLike
from pathlib import Path
from typing import override

import polars as pl


class Reader(ABC):
    path: Path

    @abstractmethod
    def read(self) -> pl.DataFrame:
        pass


class CSVReader(Reader):
    path: Path

    def __init__(self, path: PathLike[str]) -> None:
        self.path = _normalize_path(path)

    @override
    def read(self) -> pl.DataFrame:
        return pl.read_csv(self.path, encoding="latin-1", has_header=False)


def _normalize_path(path: PathLike[str]) -> Path:
    return Path(path).expanduser().resolve()


def station_code(path):
    filename = path.name
    match = re.search(r"[H|D|M]_\d{4}_(\d+)_.*", filename)
    if match is not None:
        return int(match.group(1))
    return None


def write_merged(merged, out_dir, resolution, excel, verbose):
    out_path = out_dir / f"merged_{resolution.upper()}"
    if excel:
        out_path = out_path.with_suffix(".xlsx")
        merged.write_excel(out_path)
    else:
        out_path = out_path.with_suffix(".csv")
        merged.write_csv(out_path)
    if verbose:
        print(f"Written merged file to {out_path}")


def write_individual(data, path, out_dir, excel, verbose):
    out_path = out_dir / path.name
    if excel:
        out_path = out_path.with_suffix(".xlsx")
        data.write_excel(out_path)
    else:
        data.write_csv(out_path)
    if verbose:
        print(f"Written to {out_path}")

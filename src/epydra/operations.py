from collections.abc import Sequence
from functools import cache

import polars as pl
import polars.selectors as cs

from epydra.resources import (
    load_pollutant_names,
    load_pollutant_units,
    load_stations,
)
from epydra.types import DATE_COLUMN, HOUR_COLUMN


def normalize_column_names(lf: pl.LazyFrame) -> pl.LazyFrame:
    names_map = load_pollutant_names()
    columns = lf.collect_schema().names()
    return lf.rename({c: names_map.get(c, c) for c in columns})


def filter_pollutants(lf: pl.LazyFrame, pollutants: list[str]) -> pl.LazyFrame:
    pollutants = pollutants or list(load_pollutant_units())
    return lf.select(
        cs.by_name(DATE_COLUMN, HOUR_COLUMN, *pollutants, require_all=False)
    )


@cache
def _add_unit(pollutant: str) -> str:
    meta = load_pollutant_units()[pollutant]
    unit = meta["unit"]
    if meta["phase"] == "gas":
        unit += " 293K"
    return f"{pollutant} [{unit}]"


def add_pollutant_units(lf: pl.LazyFrame) -> pl.LazyFrame:
    pollutants = set(lf.collect_schema().names()) & set(load_pollutant_units())
    return lf.rename({p: _add_unit(p) for p in pollutants})


def remove_null_columns(lf: pl.LazyFrame) -> pl.DataFrame:
    df = lf.collect()
    df = df[[col.name for col in df if col.null_count() != df.height]]
    data_cols = set(df.columns) - {DATE_COLUMN, HOUR_COLUMN}
    if not data_cols:
        return pl.DataFrame()
    return df


def merge_results(dfs: Sequence[pl.DataFrame]) -> pl.DataFrame:
    stations_data = load_stations().select("sirav", "lon", "lat")
    return pl.concat(dfs, how="diagonal_relaxed", rechunk=True).join(
        stations_data, on="sirav", how="left"
    )

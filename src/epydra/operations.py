from functools import cache

import polars as pl

from epydra.definitions import DATE_COL, HOUR_COL
from epydra.resources import load_pollutants, load_stations


def filter_pollutants(data, pollutants):
    keep = pollutants + [DATE_COL, HOUR_COL]
    cols = (c for c in data.columns if c in keep)
    return data.select(*cols)


@cache
def add_unit(pollutant):
    meta = load_pollutants()[pollutant]
    unit = meta["unit"]
    if meta["phase"] == "gas":
        unit += " 293K"
    return f"{pollutant} [{unit}]"


def format_column_names(data):
    pollutants = set(load_pollutants().keys()) & set(data.columns)
    return data.rename({p: add_unit(p) for p in pollutants})


def merge_results(dfs):
    stations_data = load_stations().select("sirav", "lon", "lat")
    return pl.concat(dfs, how="diagonal_relaxed", rechunk=True).join(
        stations_data, on="sirav", how="left"
    )

import json
from functools import cache
from importlib.resources import files
from typing import Any

import polars as pl


def _load_json(name: str) -> dict[str, Any]:
    path = files("epydra.data").joinpath(name)
    with path.open("rb") as f:
        return json.load(f)


@cache
def load_pollutant_units() -> dict[str, Any]:
    return _load_json("pollutant_units.json")


@cache
def load_pollutant_names() -> dict[str, Any]:
    return _load_json("pollutant_names.json")


@cache
def load_stations() -> pl.DataFrame:
    path = files("epydra.data").joinpath("stations.csv")
    with path.open("rb") as f:
        return pl.read_csv(f)

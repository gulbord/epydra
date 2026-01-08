import json
from functools import cache
from importlib.resources import files

import polars as pl


@cache
def load_pollutants():
    path = files("epydra.data").joinpath("pollutants.json")
    with path.open("rb") as f:
        return json.load(f)


@cache
def load_stations():
    path = files("epydra.data").joinpath("stations.csv")
    with path.open("rb") as f:
        return pl.read_csv(f)

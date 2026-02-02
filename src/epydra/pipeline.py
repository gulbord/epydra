from pathlib import Path

import polars as pl

from epydra.cleaners import make_cleaner
from epydra.io import make_reader
from epydra.operations import (
    add_pollutant_units,
    filter_pollutants,
    normalize_column_names,
    remove_null_columns,
)
from epydra.types import EsedraFilenameError


def process_file(
    path: Path, *, pollutants: list[str], with_sirav: bool = False
) -> pl.DataFrame:
    try:
        reader = make_reader(path)
        cleaner = make_cleaner(reader)
        lf = cleaner.clean(with_sirav=with_sirav)
    except (EsedraFilenameError, NotImplementedError):
        return pl.DataFrame()

    return (
        lf.pipe(normalize_column_names)
        .pipe(filter_pollutants, pollutants)
        .pipe(add_pollutant_units)
        .pipe(remove_null_columns)
    )

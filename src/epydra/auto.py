import re
import string

import polars as pl
import polars.selectors as cs

from epydra.definitions import DATE_COL, DATETIME_COL, HOUR_COL
from epydra.io import station_code


def column_names(path):
    with open(path, encoding="latin-1") as file:
        for row, line in enumerate(file):
            if row == 2:
                names = re.sub(r"\s+", "", line).split(",")
                return [n if n else str(i) for i, n in enumerate(names)]
    raise ValueError("header row not found")


def read_auto(path, resolution, with_sirav=False):
    try:
        data = pl.read_csv(path, skip_rows=4, encoding="latin-1")
        data.columns = column_names(path)
    except (OSError, ValueError):
        return pl.DataFrame()

    data = data.filter(
        pl.first().is_not_null(),
        ~pl.first().str.contains("Val", literal=True),
    )

    if resolution == "h":
        data = (
            data.rename({data.columns[0]: DATETIME_COL})
            .with_columns(
                pl.col(DATETIME_COL)
                .str.split(" ")
                .list.to_struct(fields=[DATE_COL, HOUR_COL])
            )
            .unnest(DATETIME_COL)
            .with_columns(pl.col(HOUR_COL).cast(pl.UInt8))
            .select(DATE_COL, HOUR_COL, *data.columns[1:])
        )
    elif resolution == "d":
        data = data.rename({data.columns[0]: DATE_COL})
    else:
        raise ValueError("'resolution' must be either 'h' or 'd'")

    data = (
        data.with_columns(pl.col(DATE_COL).str.to_date("%d/%m/%Y"))
        .select(~cs.matches(r"^\d+"))
        .with_columns(
            cs.string().str.strip_chars(string.whitespace).replace("", None)
        )
    )

    data = data[[col.name for col in data if col.null_count() != data.height]]

    if with_sirav:
        code = pl.repeat(station_code(path), data.height).alias("sirav")
        data.insert_column(2 if resolution == "h" else 1, code)

    return data[[col.name for col in data if col.null_count() != data.height]]

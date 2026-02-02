import re
import string
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import override

import polars as pl
import polars.selectors as cs
from polars.exceptions import OutOfBoundsError

from epydra.io import Reader
from epydra.types import DATE_COLUMN, HOUR_COLUMN, SIRAV_COLUMN


class Cleaner(ABC):
    reader: Reader

    def __init__(self, reader: Reader) -> None:
        self.reader = reader

    @abstractmethod
    def clean(self, *, with_sirav: bool) -> pl.LazyFrame: ...


class AutomaticCleaner(Cleaner):
    reader: Reader

    def _remove_extra_rows(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.slice(4)

    def _remove_extra_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.select(~cs.matches(r"^\d+"))

    def _filter_dates(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.filter(
            pl.first().is_not_null(),
            ~pl.first().str.contains("Val", literal=True),
        )

    def _make_time_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        columns = lf.collect_schema().names()
        has_hours = (
            lf.select(pl.first().str.contains(" ").any()).collect().item()
        )
        if has_hours:
            return (
                lf.with_columns(
                    pl.first()
                    .str.split(" ")
                    .list.to_struct(fields=[DATE_COLUMN, HOUR_COLUMN])
                )
                .unnest(cs.struct())
                .with_columns(pl.col(HOUR_COLUMN).cast(pl.UInt8))
                .select(DATE_COLUMN, HOUR_COLUMN, *columns[1:])
            )
        return lf.rename({columns[0]: DATE_COLUMN})

    @override
    def clean(self, *, with_sirav: bool = False) -> pl.LazyFrame:
        try:
            data = self.reader.read()
            data.columns = _extract_column_names(data, skip=2)
        except (OSError, ValueError):
            return pl.LazyFrame()

        data = (
            data.lazy()
            .pipe(self._remove_extra_rows)
            .pipe(_fix_string_nulls)
            .pipe(self._filter_dates)
            .pipe(self._make_time_columns)
            .pipe(_format_date_column)
            .pipe(self._remove_extra_columns)
        )

        return (
            data.pipe(_make_sirav_column, self.reader.sirav_code)
            if with_sirav
            else data
        )


class ManualCleaner(Cleaner):
    reader: Reader

    def _remove_extra_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.slice(3)
        if "Note chimica" in df.columns:
            return df.filter(pl.col("Note chimica") == " ")
        return df

    def _remove_extra_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.select(
            cs.exclude(
                cs.by_index(0, 2, 3),
                cs.by_name("Peso", "Volume", "Note chimica", require_all=False),
            )
        )

    def _make_date_column(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.select(
            pl.first().str.split(" ").list.first().alias(DATE_COLUMN),
            cs.exclude(cs.first()),
        )

    def _group_by_date(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.group_by(DATE_COLUMN, maintain_order=True).agg(
            cs.all().last(ignore_nulls=True)
        )

    @override
    def clean(self, *, with_sirav: bool = False) -> pl.LazyFrame:
        try:
            data = self.reader.read()
            data.columns = _extract_column_names(data, skip=0)
        except (OSError, ValueError):
            return pl.LazyFrame()

        data = self._remove_extra_rows(data)
        if data.is_empty():
            return data.lazy()

        data = (
            data.lazy()
            .pipe(self._remove_extra_columns)
            .pipe(_fix_string_nulls)
            .pipe(self._make_date_column)
            .pipe(_format_date_column)
            .pipe(self._group_by_date)
        )

        return (
            data.pipe(_make_sirav_column, self.reader.sirav_code)
            if with_sirav
            else data
        )


def _extract_column_names(df: pl.DataFrame, skip: int) -> list[str]:
    try:
        header = df.row(skip)
    except OutOfBoundsError:
        raise ValueError("header row not found")

    names = [
        re.sub(r"\s+", "", x or "") or str(i) for i, x in enumerate(header)
    ]

    seen: dict[str, int] = defaultdict(int)
    final: list[str] = []
    for name in names:
        count = seen[name]
        final.append(name if not count else f"{name}_{count}")
        seen[name] += 1
        seen[final[-1]] += 0

    return final


def _format_date_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(pl.col(DATE_COLUMN).str.to_date("%d/%m/%Y"))


def _fix_string_nulls(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        cs.string().str.strip_chars(string.whitespace).replace("", None)
    )


def _make_sirav_column(lf: pl.LazyFrame, sirav_code: int) -> pl.LazyFrame:
    return lf.select(pl.lit(sirav_code).alias(SIRAV_COLUMN), cs.all())


def make_cleaner(reader: Reader) -> Cleaner:
    if reader.path.name[0] != "M":
        return AutomaticCleaner(reader)
    return ManualCleaner(reader)

import argparse
import concurrent.futures as cf
from pathlib import Path

import polars as pl

from epydra.io import write_dataframe
from epydra.operations import merge_results
from epydra.pipeline import process_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean up automatic station data from ESEDRA"
    )
    _ = parser.add_argument("in_dir", type=Path, help="Input directory")
    _ = parser.add_argument("out_dir", type=Path, help="Output directory")
    _ = parser.add_argument(
        "-m",
        "--merge",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Merge into a single file with geolocalization?",
    )
    _ = parser.add_argument(
        "-x",
        "--excel",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Save output as Excel files?",
    )
    _ = parser.add_argument(
        "-v",
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Log to standard output?",
    )
    _ = parser.add_argument(
        "-p",
        "--pollutants",
        nargs="+",
        default=[],
        help="Keep only some pollutants",
    )

    args = parser.parse_args()
    _ = args.out_dir.mkdir(parents=True, exist_ok=True)

    with cf.ProcessPoolExecutor() as executor:
        futures: dict[cf.Future[pl.DataFrame], Path] = {
            executor.submit(
                process_file,
                path=path,
                pollutants=args.pollutants,
                with_sirav=args.merge,
            ): path
            for path in args.in_dir.iterdir()
            if path.is_file()
        }
        results: list[tuple[Path, pl.DataFrame]] = []
        for f in cf.as_completed(futures):
            path = futures[f]
            try:
                results.append((path, f.result()))
            except Exception as e:
                raise RuntimeError(f"failed processing {path}") from e

    if not results:
        raise RuntimeError("all input files were empty!")

    out_format = "xlsx" if args.excel else "csv"
    if args.merge:
        merged = merge_results([data for _, data in results])
        _ = write_dataframe(
            merged,
            args.out_dir / "merged",
            format=out_format,
            verbose=args.verbose,
        )
    else:
        for path, data in results:
            _ = write_dataframe(
                data,
                args.out_dir / path.name,
                format=out_format,
                verbose=args.verbose,
            )


if __name__ == "__main__":
    main()

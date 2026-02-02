import argparse
import concurrent.futures as cf
from pathlib import Path

from epydra.auto import read_auto
from epydra.io import write_individual, write_merged
from epydra.operations import (
    filter_pollutants,
    format_column_names,
    merge_results,
)


def clean_file(path, resolution, pollutants, with_sirav, verbose):
    data = read_auto(path, resolution, with_sirav)
    if data.is_empty():
        if verbose:
            print(f"Skipping empty file {path}")
        return
    if pollutants is not None:
        data = filter_pollutants(data, pollutants=pollutants)
    return format_column_names(data)


def main():
    parser = argparse.ArgumentParser(
        description="Clean up automatic station data from ESEDRA"
    )
    parser.add_argument("in_dir", type=Path, help="Input directory")
    parser.add_argument("out_dir", type=Path, help="Output directory")
    parser.add_argument(
        "resolution", choices=("d", "h"), help="[d]aily or [h]ourly data?"
    )
    parser.add_argument(
        "-m",
        "--merge",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Merge into a single file with geolocalization?",
    )
    parser.add_argument(
        "-x",
        "--excel",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Save output as Excel files?",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Log to standard output?",
    )
    parser.add_argument(
        "-p",
        "--pollutants",
        nargs="+",
        default=None,
        help="Keep only some pollutants",
    )

    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    files = list(args.in_dir.glob(f"{args.resolution.upper()}_*.csv"))
    if not files:
        raise FileNotFoundError(f"no matching files found in {args.in_dir}")

    with cf.ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(
                clean_file,
                path=path,
                resolution=args.resolution,
                pollutants=args.pollutants,
                with_sirav=args.merge,
                verbose=args.verbose,
            ): path
            for path in files
        }
        results = [(futures[f], f.result()) for f in cf.as_completed(futures)]

    if not results:
        raise RuntimeError("all input files were empty!")

    if args.merge:
        merged = merge_results([data for _, data in results])
        write_merged(
            merged,
            out_dir=args.out_dir,
            resolution=args.resolution,
            excel=args.excel,
            verbose=args.verbose,
        )
    else:
        for path, data in results:
            write_individual(
                data,
                path,
                out_dir=args.out_dir,
                excel=args.excel,
                verbose=args.verbose,
            )


if __name__ == "__main__":
    main()

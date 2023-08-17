import pandas as pd
import argparse
import glob


def main(args: argparse.Namespace) -> None:
    paths = [l for l in glob.glob(f"tables/*{args.suffix}.csv")]
    list_df = [pd.read_csv(p, delimiter=";") for p in paths]
    df = pd.concat(list_df, ignore_index=True)
    if args.print:
        print(df.head())
        print(df.shape)
    df.to_csv(args.name, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read indices and generate CSV files")
    parser.add_argument(
        "--suffix",
        default="tokyo",
        help="suffix used in output filenames",
    )
    parser.add_argument(
        "-n",
        "--name",
        default="tokyo.csv",
        help="output filename",
    )
    parser.add_argument("--print", action="store_true", default=False)
    args = parser.parse_args()
    main(args)

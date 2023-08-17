from pyarrow.parquet import read_table
from tqdm import tqdm
import codecs
import dill
import os.path
import argparse

# vis
import matplotlib.pyplot as plt

plt.style.use("ggplot")


def main(
    args: argparse.Namespace, data_dir: str = "~/data/Overture/type=place"
) -> None:
    data_dir = os.path.expanduser(data_dir)

    # parquet file
    fn_base = args.name
    fn = os.path.join(data_dir, fn_base)
    table = read_table(fn)

    # index file
    fn_idx = os.path.join("indices", f"{fn_base}_index_{args.suffix}.dill")
    idx = dill.load(open(fn_idx, "rb"))

    if args.print:
        print(table.shape)
        print(len(idx))

    fn_table = os.path.join("tables", f"{fn_base}_{args.suffix}.csv")
    with codecs.open(fn_table, "w", encoding="utf-8") as ff:
        ff.write('"name";"cat";"lon";"lat"\n')
        for i in tqdm(idx):
            data_i = {}
            for j in table.column_names:
                data_i[j] = table[j][i].as_py()
            name_i = data_i["names"][0][1][0][0][1]
            cat_i = data_i["categories"]["main"]
            lon_i = data_i["bbox"]["minx"]
            lat_i = data_i["bbox"]["miny"]
            ff.write(f'"{name_i}";"{cat_i}";"{lon_i}";"{lat_i}"\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read indices and generate CSV files")
    parser.add_argument(
        "-n",
        "--name",
        default="20230725_210643_00079_ayc64_01c760ca-02aa-4387-8b71-b2eaa6c7c700",
        help="input parquet filename",
    )
    parser.add_argument(
        "--suffix",
        default="tokyo",
        help="suffix used in output filenames",
    )
    parser.add_argument("--print", action="store_true", default=False)
    args = parser.parse_args()
    main(args)

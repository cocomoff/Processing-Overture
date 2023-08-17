from pyarrow.parquet import read_table
from shapely.geometry import point
from tqdm import tqdm
import dill
import os.path

# vis
import matplotlib.pyplot as plt

plt.style.use("ggplot")

data_dir = "~/data/Overture/type=place"
data_dir = os.path.expanduser(data_dir)

# shape file
sp = dill.load(open("shapes/N03-21_13_210101.dill", "rb"))

# parquet file
fn_base = "20230725_210643_00079_ayc64_01c760ca-02aa-4387-8b71-b2eaa6c7c700"
fn = os.path.join(data_dir, fn_base)
table = read_table(fn)
print(table.shape)

rem_idx = []
rem_lat = []
rem_lon = []

for j in tqdm(range(table.shape[0])):
    lat_j = table["bbox"][j]["miny"].as_py()
    lon_j = table["bbox"][j]["minx"].as_py()
    pj = point.Point(lon_j, lat_j)
    if sp.contains(pj):
        rem_idx.append(j)
        rem_lat.append(lat_j)
        rem_lon.append(lon_j)

# files (index)
fn_idx = os.path.join("indices", f"{fn_base}_index.dill")
with open(fn_idx, "wb") as f:
    dill.dump(rem_idx, f)

# figures
f = plt.figure(figsize=(8, 5))
a = f.gca()
a.scatter(rem_lon, rem_lat, marker="x", color="dodgerblue", s=5)
plt.tight_layout()
plt.savefig(f"figures/{fn_base}_tokyo.png", dpi=150)
plt.close()

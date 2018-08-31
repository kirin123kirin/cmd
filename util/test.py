from util import read_csv, Path
import pandas as pd

f = Path(r"C:\temp\a.csv")

df = pd.read_csv(open(f), sep="\n", dtype=bytes, names=["line"])

print(df.dtypes)
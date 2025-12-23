import pandas as pd
import numpy as np

filepath = r"database\csvs\ARTICULOS.csv"
try:
    df = pd.read_csv(filepath, encoding="utf-8-sig", dtype=str, on_bad_lines='skip')
    print("Columns:", df.columns.tolist())
    for col in df.columns:
        print(f"'{col}' (len={len(col)})")
except Exception as e:
    print(f"Error: {e}")

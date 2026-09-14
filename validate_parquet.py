import pandas as pd

df = pd.read_parquet(
    "output/sales.parquet",
    engine="pyarrow"
)

print("=== INFORMACIÓN DEL PARQUET ===")
print(f"Registros: {len(df)}")
print(f"Columnas: {len(df.columns)}")

print("\n=== COLUMNAS ===")
print(df.columns.tolist())

print("\n=== TIPOS ===")
print(df.dtypes)

print("\n=== PRIMEROS REGISTROS ===")
print(df.head(10))
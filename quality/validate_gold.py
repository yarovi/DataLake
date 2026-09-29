from pathlib import Path


GOLD_BASE_PATH = Path("staging/gold")

GOLD_DATASETS = [
    "sales_by_country",
    "sales_by_month",
    "sales_by_product",
]


def validate_dataset(dataset: str) -> None:
    dataset_path = GOLD_BASE_PATH / dataset

    print(f"\n[DQ] Validando: {dataset}")

    # 1. El directorio debe existir
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"[DQ] ERROR - No existe el dataset: {dataset_path}"
        )

    # 2. Debe existir al menos un archivo Parquet
    parquet_files = list(dataset_path.glob("part-*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"[DQ] ERROR - No existen archivos Parquet en: {dataset_path}"
        )

    print(f"[DQ] Parquet encontrados: {len(parquet_files)}")

    # 3. Ningún Parquet debe estar vacío
    for parquet_file in parquet_files:
        size = parquet_file.stat().st_size

        print(
            f"[DQ] Archivo: {parquet_file.name} "
            f"({size} bytes)"
        )

        if size == 0:
            raise ValueError(
                f"[DQ] ERROR - Archivo vacío: {parquet_file}"
            )

    # 4. Spark debe haber terminado correctamente la escritura
    success_file = dataset_path / "_SUCCESS"

    if not success_file.exists():
        raise FileNotFoundError(
            f"[DQ] ERROR - Falta _SUCCESS en: {dataset_path}"
        )

    print(f"[DQ] OK - {dataset}")


def main() -> None:
    print("=== DATA QUALITY - GOLD ===")

    for dataset in GOLD_DATASETS:
        validate_dataset(dataset)

    print("\n[DQ] TODAS LAS VALIDACIONES ESTRUCTURALES PASARON")


if __name__ == "__main__":
    main()
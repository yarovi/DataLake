from elt.extract_incremental import extract_orders_after
from elt.load_bronze_v5 import load_incremental_to_bronze
from state.watermark import read_watermark


def main():
    current_watermark = read_watermark()

    print(
        f"[INCREMENTAL] Watermark actual: "
        f"{current_watermark}"
    )

    orders = extract_orders_after(current_watermark)

    print(
        f"[INCREMENTAL] Registros encontrados: "
        f"{len(orders)}"
    )

    if orders.empty:
        print("[INCREMENTAL] No existen nuevas órdenes.")
        print("[INCREMENTAL] No hay nada que procesar.")
        return

    start_order_id = int(orders["order_id"].min())
    candidate_watermark = int(orders["order_id"].max())

    print(
        f"[INCREMENTAL] Rango detectado: "
        f"{start_order_id} -> {candidate_watermark}"
    )

    print(
        f"[INCREMENTAL] Watermark candidato: "
        f"{candidate_watermark}"
    )

    blob_name = load_incremental_to_bronze(
        dataframe=orders,
        table_name="orders",
        start_order_id=start_order_id,
        end_order_id=candidate_watermark
    )

    print(
        f"[INCREMENTAL] Bronze generado: "
        f"{blob_name}"
    )

    print(
        "[INCREMENTAL] Watermark todavía NO actualizado."
    )


if __name__ == "__main__":
    main()
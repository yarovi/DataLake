from elt.extract_incremental import extract_orders_after
from state.watermark import read_watermark


def main():
    current_watermark = read_watermark()

    print(f"[INCREMENTAL] Watermark actual: {current_watermark}")

    orders = extract_orders_after(current_watermark)

    print(f"[INCREMENTAL] Registros encontrados: {len(orders)}")

    if orders.empty:
        print("[INCREMENTAL] No existen nuevas órdenes.")
        print("[INCREMENTAL] No hay nada que procesar.")
        return

    candidate_watermark = int(orders["order_id"].max())

    print(
        f"[INCREMENTAL] Rango detectado: "
        f"{orders['order_id'].min()} -> {orders['order_id'].max()}"
    )

    print(
        f"[INCREMENTAL] Watermark candidato: "
        f"{candidate_watermark}"
    )

    print(
        "[INCREMENTAL] IMPORTANTE: "
        "el watermark todavía NO será actualizado."
    )


if __name__ == "__main__":
    main()
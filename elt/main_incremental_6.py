from elt.extract_incremental import (
    extract_orders_after,
    extract_order_details_after
)
from elt.load_bronze_v5 import load_incremental_to_bronze
from state.watermark import read_watermark


def main():
    current_watermark = read_watermark()

    print(
        f"[INCREMENTAL] Watermark actual: "
        f"{current_watermark}"
    )

    # 1. Extraer nuevas órdenes
    orders = extract_orders_after(current_watermark)

    print(
        f"[INCREMENTAL] Orders encontrados: "
        f"{len(orders)}"
    )

    if orders.empty:
        print("[INCREMENTAL] No existen nuevas órdenes.")
        print("[INCREMENTAL] Pipeline sin cambios.")
        return

    start_order_id = int(orders["order_id"].min())
    candidate_watermark = int(orders["order_id"].max())

    print(
        f"[INCREMENTAL] Rango: "
        f"{start_order_id} -> {candidate_watermark}"
    )

    # 2. Extraer detalles del mismo rango incremental
    order_details = extract_order_details_after(
        current_watermark
    )

    print(
        f"[INCREMENTAL] Order details encontrados: "
        f"{len(order_details)}"
    )

    # 3. Validación básica de integridad del batch
    order_ids = set(orders["order_id"])
    detail_order_ids = set(order_details["order_id"])

    missing_details = order_ids - detail_order_ids
    orphan_details = detail_order_ids - order_ids

    if missing_details:
        raise ValueError(
            f"[INCREMENTAL] Orders sin details: "
            f"{sorted(missing_details)}"
        )

    if orphan_details:
        raise ValueError(
            f"[INCREMENTAL] Details sin order: "
            f"{sorted(orphan_details)}"
        )

    print(
        "[INCREMENTAL] Integridad orders/order_details: OK"
    )

    # 4. Bronze incremental: orders
    orders_blob = load_incremental_to_bronze(
        dataframe=orders,
        table_name="orders",
        start_order_id=start_order_id,
        end_order_id=candidate_watermark
    )

    # 5. Bronze incremental: order_details
    details_blob = load_incremental_to_bronze(
        dataframe=order_details,
        table_name="order_details",
        start_order_id=start_order_id,
        end_order_id=candidate_watermark
    )

    print()
    print("[INCREMENTAL] Batch Bronze generado:")
    print(f"  Orders       : {orders_blob}")
    print(f"  Order details: {details_blob}")

    print(
        f"[INCREMENTAL] Watermark candidato: "
        f"{candidate_watermark}"
    )

    print(
        "[INCREMENTAL] Watermark todavía NO actualizado."
    )


if __name__ == "__main__":
    main()
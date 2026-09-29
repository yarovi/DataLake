from pathlib import Path


STATE_DIR = Path(__file__).resolve().parent
WATERMARK_FILE = STATE_DIR / "orders_watermark.txt"


def read_watermark(default: int = 0) -> int:
    if not WATERMARK_FILE.exists():
        return default

    value = WATERMARK_FILE.read_text().strip()

    if not value:
        return default

    return int(value)


def write_watermark(order_id: int) -> None:
    WATERMARK_FILE.write_text(str(order_id))
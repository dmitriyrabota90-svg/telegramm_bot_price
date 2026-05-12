import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path

MPL_CONFIG_DIR = Path("Logs") / "matplotlib"
MPL_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CONFIG_DIR))

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


logger = logging.getLogger(__name__)


def build_price_chart(product_title: str, snapshots: list[dict]) -> str:
    points = []
    for snapshot in snapshots:
        try:
            points.append(
                (
                    datetime.fromisoformat(snapshot["fetched_at"]),
                    float(snapshot["price"]),
                )
            )
        except (TypeError, ValueError):
            logger.warning(
                "Skipping invalid chart point product_title=%s snapshot_id=%s",
                product_title,
                snapshot.get("id"),
            )

    if len(points) < 2:
        raise ValueError("Недостаточно данных для построения графика")

    dates = [point[0] for point in points]
    prices = [point[1] for point in points]

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(dates, prices, marker="o", linewidth=2)
    ax.set_title(f"{product_title}: цена за последние 7 дней")
    ax.set_xlabel("Дата и время")
    ax.set_ylabel("Цена")
    ax.grid(True, alpha=0.25)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
    fig.autofmt_xdate()
    fig.tight_layout()

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    temp_path = Path(temp_file.name)
    temp_file.close()

    fig.savefig(temp_path, dpi=150)
    plt.close(fig)

    logger.info("Price chart generated product_title=%s path=%s points=%s", product_title, temp_path, len(points))
    return str(temp_path)

import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional


DEFAULT_DB_PATH = "prices.db"

logger = logging.getLogger(__name__)


def get_db_path() -> str:
    return os.getenv("SQLITE_DB_PATH", DEFAULT_DB_PATH)


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    resolved_db_path = Path(db_path or get_db_path()).expanduser()
    db_parent = resolved_db_path.parent
    if str(db_parent) not in ("", "."):
        db_parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(resolved_db_path))
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db(products: list[dict], db_path: Optional[str] = None) -> None:
    resolved_db_path = str(Path(db_path or get_db_path()).expanduser())
    logger.info("Initializing SQLite database path=%s", resolved_db_path)

    with get_connection(resolved_db_path) as connection:
        create_tables(connection)
        sync_products(connection, products)

    logger.info("SQLite database initialized path=%s", resolved_db_path)


def create_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS products (
            key TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            source_name TEXT,
            is_active INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS price_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_key TEXT NOT NULL,
            product_title TEXT NOT NULL,
            price REAL,
            fetched_at TEXT NOT NULL,
            source_name TEXT,
            run_type TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (product_key) REFERENCES products(key)
        );

        CREATE TABLE IF NOT EXISTS fetch_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            run_type TEXT NOT NULL,
            total_products INTEGER NOT NULL,
            success_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS allowed_users (
            telegram_user_id INTEGER PRIMARY KEY,
            username TEXT,
            is_active INTEGER NOT NULL DEFAULT 1
        );
        """
    )


def sync_products(connection: sqlite3.Connection, products: list[dict]) -> None:
    rows = [
        (
            product["key"],
            product["title"],
            product.get("source_name"),
        )
        for product in products
    ]

    connection.executemany(
        """
        INSERT OR IGNORE INTO products (key, title, source_name)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    logger.info("Products synchronized with SQLite count=%s", len(rows))


def get_active_product_keys() -> set[str]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT key
            FROM products
            WHERE is_active = 1
            """
        ).fetchall()

    return {row[0] for row in rows}


def get_active_products_count() -> int:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM products
            WHERE is_active = 1
            """
        ).fetchone()

    return int(row[0])


def sync_allowed_users(user_ids: list[int]) -> None:
    if not user_ids:
        logger.warning("No initial allowed Telegram users configured")
        return

    rows = [(user_id,) for user_id in user_ids]
    with get_connection() as connection:
        connection.executemany(
            """
            INSERT OR IGNORE INTO allowed_users (telegram_user_id)
            VALUES (?)
            """,
            rows,
        )

    logger.info("Allowed Telegram users synchronized count=%s", len(rows))


def is_user_allowed(telegram_user_id: int) -> bool:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM allowed_users
            WHERE telegram_user_id = ?
              AND is_active = 1
            LIMIT 1
            """,
            (telegram_user_id,),
        ).fetchone()

    return row is not None


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[dict]:
    if row is None:
        return None
    return dict(row)


def get_latest_success_snapshots() -> dict[str, dict]:
    with get_connection() as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT ps.*
            FROM price_snapshots ps
            INNER JOIN (
                SELECT product_key, MAX(id) AS latest_id
                FROM price_snapshots
                WHERE status = 'success'
                GROUP BY product_key
            ) latest
                ON latest.latest_id = ps.id
            """
        ).fetchall()

    return {row["product_key"]: dict(row) for row in rows}


def get_previous_success_snapshot(product_key: str, latest_snapshot_id: int) -> Optional[dict]:
    with get_connection() as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            """
            SELECT *
            FROM price_snapshots
            WHERE product_key = ?
              AND status = 'success'
              AND id < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (product_key, latest_snapshot_id),
        ).fetchone()

    return _row_to_dict(row)


def get_two_latest_success_snapshots(product_key: str) -> list[dict]:
    with get_connection() as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT *
            FROM price_snapshots
            WHERE product_key = ?
              AND status = 'success'
            ORDER BY id DESC
            LIMIT 2
            """,
            (product_key,),
        ).fetchall()

    return [dict(row) for row in rows]


def get_success_snapshots_for_product_since(product_key: str, since: str) -> list[dict]:
    with get_connection() as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT *
            FROM price_snapshots
            WHERE product_key = ?
              AND status = 'success'
              AND fetched_at >= ?
            ORDER BY fetched_at ASC, id ASC
            """,
            (product_key, since),
        ).fetchall()

    return [dict(row) for row in rows]


def get_latest_fetch_run(run_type: Optional[str] = None) -> Optional[dict]:
    query = """
        SELECT *
        FROM fetch_runs
    """
    params = []
    if run_type is not None:
        query += " WHERE run_type = ?"
        params.append(run_type)

    query += " ORDER BY id DESC LIMIT 1"

    with get_connection() as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(query, params).fetchone()

    return _row_to_dict(row)


def get_latest_successful_fetch_run(run_type: str) -> Optional[dict]:
    with get_connection() as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            """
            SELECT *
            FROM fetch_runs
            WHERE run_type = ?
              AND success_count > 0
            ORDER BY id DESC
            LIMIT 1
            """,
            (run_type,),
        ).fetchone()

    return _row_to_dict(row)


def get_error_snapshots_for_run_window(run: dict) -> list[dict]:
    finished_at = run.get("finished_at") or datetime_now_iso()
    with get_connection() as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT *
            FROM price_snapshots
            WHERE run_type = ?
              AND status = 'error'
              AND created_at >= ?
              AND created_at <= ?
            ORDER BY id ASC
            """,
            (run["run_type"], run["started_at"], finished_at),
        ).fetchall()

    return [dict(row) for row in rows]


def datetime_now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def create_fetch_run(
    run_type: str,
    total_products: int,
    started_at: str,
    notes: Optional[str] = None,
) -> int:
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO fetch_runs (started_at, run_type, total_products, notes)
            VALUES (?, ?, ?, ?)
            """,
            (started_at, run_type, total_products, notes),
        )
        run_id = cursor.lastrowid

    logger.info("Created fetch run id=%s run_type=%s total_products=%s", run_id, run_type, total_products)
    return run_id


def finish_fetch_run(
    run_id: int,
    finished_at: str,
    success_count: int,
    error_count: int,
    total_products: int,
) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            UPDATE fetch_runs
            SET finished_at = ?,
                success_count = ?,
                error_count = ?,
                total_products = ?
            WHERE id = ?
            """,
            (finished_at, success_count, error_count, total_products, run_id),
        )

    logger.info(
        "Finished fetch run id=%s success_count=%s error_count=%s total_products=%s",
        run_id,
        success_count,
        error_count,
        total_products,
    )


def save_price_snapshot(snapshot: dict) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO price_snapshots (
                product_key,
                product_title,
                price,
                fetched_at,
                source_name,
                run_type,
                status,
                error_message,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot["product_key"],
                snapshot["product_title"],
                snapshot["price"],
                snapshot["fetched_at"],
                snapshot["source_name"],
                snapshot["run_type"],
                snapshot["status"],
                snapshot["error_message"],
                snapshot["created_at"],
            ),
        )

    logger.info(
        "Saved price snapshot product_key=%s status=%s run_type=%s",
        snapshot["product_key"],
        snapshot["status"],
        snapshot["run_type"],
    )


def save_price_snapshots(snapshots: list[dict]) -> None:
    if not snapshots:
        return

    rows = [
        (
            snapshot["product_key"],
            snapshot["product_title"],
            snapshot["price"],
            snapshot["fetched_at"],
            snapshot["source_name"],
            snapshot["run_type"],
            snapshot["status"],
            snapshot["error_message"],
            snapshot["created_at"],
        )
        for snapshot in snapshots
    ]

    with get_connection() as connection:
        connection.executemany(
            """
            INSERT INTO price_snapshots (
                product_key,
                product_title,
                price,
                fetched_at,
                source_name,
                run_type,
                status,
                error_message,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    logger.info("Saved price snapshots count=%s", len(rows))

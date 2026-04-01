"""
setup_db.py
-----------
Creates the SQLite3 database and seeds it with sample data that satisfies
the test case described in the assignment:

  Customer 1 (age 21) → Item x × 10  (multiple transactions)
  Customer 2 (age 23) → Item x × 1, Item y × 1, Item z × 1
  Customer 3 (age 35) → Item z × 2   (two occasions)
  Customer 4 (age 40) → outside 18-35 range → must be excluded from output
"""

import logging
import os
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = "sales.db"


def create_schema(conn: sqlite3.Connection) -> None:
    logger.info("Creating schema...")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS Customer (
            customer_id INTEGER PRIMARY KEY,
            age         INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Items (
            item_id   INTEGER PRIMARY KEY,
            item_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Sales (
            sales_id    INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
        );

        CREATE TABLE IF NOT EXISTS Orders (
            order_id  INTEGER PRIMARY KEY,
            sales_id  INTEGER NOT NULL,
            item_id   INTEGER NOT NULL,
            quantity  INTEGER,          -- NULL means item was not bought
            FOREIGN KEY (sales_id) REFERENCES Sales(sales_id),
            FOREIGN KEY (item_id)  REFERENCES Items(item_id)
        );
    """)
    conn.commit()
    logger.info("Schema created.")


def seed_data(conn: sqlite3.Connection) -> None:
    logger.info("Seeding data...")

    customers = [
        (1, 21),   # in range
        (2, 23),   # in range
        (3, 35),   # in range (boundary)
        (4, 40),   # OUT of range – must not appear in output
    ]
    items = [
        (1, "x"),
        (2, "y"),
        (3, "z"),
    ]
    sales = [
        (1, 1), (2, 1), (3, 1),   # Customer 1 — 3 receipts
        (4, 2),                    # Customer 2 — 1 receipt
        (5, 3), (6, 3),            # Customer 3 — 2 receipts
        (7, 4),                    # Customer 4 — excluded by age
    ]
    # Clerk records ALL items per receipt; not-bought rows use quantity=NULL.
    # Customer 1: x=4+3+3=10, y/z=NULL throughout
    # Customer 2: x=1, y=1, z=1
    # Customer 3: x/y=NULL throughout, z=1+1=2
    # Customer 4: x=2 (filtered out)
    orders = [
        (1,  1, 1, 4),    (2,  1, 2, None), (3,  1, 3, None),
        (4,  2, 1, 3),    (5,  2, 2, None), (6,  2, 3, None),
        (7,  3, 1, 3),    (8,  3, 2, None), (9,  3, 3, None),
        (10, 4, 1, 1),    (11, 4, 2, 1),    (12, 4, 3, 1),
        (13, 5, 1, None), (14, 5, 2, None), (15, 5, 3, 1),
        (16, 6, 1, None), (17, 6, 2, None), (18, 6, 3, 1),
        (19, 7, 1, 2),    (20, 7, 2, None), (21, 7, 3, None),
    ]

    conn.executemany("INSERT OR IGNORE INTO Customer VALUES (?, ?)", customers)
    conn.executemany("INSERT OR IGNORE INTO Items   VALUES (?, ?)", items)
    conn.executemany("INSERT OR IGNORE INTO Sales   VALUES (?, ?)", sales)
    conn.executemany("INSERT OR IGNORE INTO Orders  VALUES (?, ?, ?, ?)", orders)
    conn.commit()
    logger.info(
        "Seeded %d customers, %d items, %d sales, %d order lines.",
        len(customers), len(items), len(sales), len(orders),
    )


def main() -> None:
    if os.path.exists(DB_PATH):
        logger.warning("Existing '%s' found — removing for a fresh seed.", DB_PATH)
        os.remove(DB_PATH)

    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as exc:
        logger.error("Could not open database '%s': %s", DB_PATH, exc)
        raise

    try:
        create_schema(conn)
        seed_data(conn)
    except sqlite3.Error as exc:
        logger.error("Database operation failed: %s", exc)
        raise
    finally:
        conn.close()
        logger.info("Connection closed.")

    logger.info("Database '%s' is ready.", DB_PATH)


if __name__ == "__main__":
    main()
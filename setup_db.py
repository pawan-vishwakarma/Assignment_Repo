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

import sqlite3
import os

DB_PATH = "sales.db"


def create_schema(conn: sqlite3.Connection) -> None:
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


def seed_data(conn: sqlite3.Connection) -> None:
    # ── Customers ────────────────────────────────────────────────────────────
    customers = [
        (1, 21),   # in range
        (2, 23),   # in range
        (3, 35),   # in range (boundary)
        (4, 40),   # OUT of range – must not appear in output
    ]

    # ── Items ─────────────────────────────────────────────────────────────────
    items = [
        (1, "x"),
        (2, "y"),
        (3, "z"),
    ]

    # ── Sales receipts ────────────────────────────────────────────────────────
    # Customer 1 has 3 receipts (all for item x), clerk records all 3 items each time
    # Customer 2 has 1 receipt
    # Customer 3 has 2 receipts (for item z), clerk records all 3 items each time
    # Customer 4 has 1 receipt (should be filtered out by age)
    sales = [
        (1,  1),   # receipt 1  → Customer 1
        (2,  1),   # receipt 2  → Customer 1
        (3,  1),   # receipt 3  → Customer 1
        (4,  2),   # receipt 4  → Customer 2
        (5,  3),   # receipt 5  → Customer 3
        (6,  3),   # receipt 6  → Customer 3
        (7,  4),   # receipt 7  → Customer 4 (out-of-range)
    ]

    # ── Orders ────────────────────────────────────────────────────────────────
    # Business rule: clerk records ALL items per receipt; not-bought = NULL
    # Customer 1: receipt 1 → x=4, y=NULL, z=NULL
    #             receipt 2 → x=3, y=NULL, z=NULL
    #             receipt 3 → x=3, y=NULL, z=NULL   → total x = 10 ✓
    # Customer 2: receipt 4 → x=1, y=1,    z=1     → total x=1, y=1, z=1 ✓
    # Customer 3: receipt 5 → x=NULL, y=NULL, z=1
    #             receipt 6 → x=NULL, y=NULL, z=1   → total z = 2 ✓
    # Customer 4: receipt 7 → x=2, y=NULL, z=NULL   (excluded by age filter)
    orders = [
        # sales_id=1 (Customer 1, receipt 1)
        (1,  1, 1, 4),      # x = 4
        (2,  1, 2, None),   # y = NULL
        (3,  1, 3, None),   # z = NULL
        # sales_id=2 (Customer 1, receipt 2)
        (4,  2, 1, 3),      # x = 3
        (5,  2, 2, None),   # y = NULL
        (6,  2, 3, None),   # z = NULL
        # sales_id=3 (Customer 1, receipt 3)
        (7,  3, 1, 3),      # x = 3
        (8,  3, 2, None),   # y = NULL
        (9,  3, 3, None),   # z = NULL
        # sales_id=4 (Customer 2, receipt 1)
        (10, 4, 1, 1),      # x = 1
        (11, 4, 2, 1),      # y = 1
        (12, 4, 3, 1),      # z = 1
        # sales_id=5 (Customer 3, receipt 1)
        (13, 5, 1, None),   # x = NULL
        (14, 5, 2, None),   # y = NULL
        (15, 5, 3, 1),      # z = 1
        # sales_id=6 (Customer 3, receipt 2)
        (16, 6, 1, None),   # x = NULL
        (17, 6, 2, None),   # y = NULL
        (18, 6, 3, 1),      # z = 1
        # sales_id=7 (Customer 4, receipt 1) – age 40, must be excluded
        (19, 7, 1, 2),      # x = 2
        (20, 7, 2, None),   # y = NULL
        (21, 7, 3, None),   # z = NULL
    ]

    conn.executemany("INSERT OR IGNORE INTO Customer VALUES (?, ?)", customers)
    conn.executemany("INSERT OR IGNORE INTO Items   VALUES (?, ?)", items)
    conn.executemany("INSERT OR IGNORE INTO Sales   VALUES (?, ?)", sales)
    conn.executemany(
        "INSERT OR IGNORE INTO Orders VALUES (?, ?, ?, ?)", orders
    )
    conn.commit()
    print(f"[setup_db] Database '{DB_PATH}' ready with sample data.")


def main() -> None:
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)          # fresh start each run
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    seed_data(conn)
    conn.close()


if __name__ == "__main__":
    main()

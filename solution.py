"""
main.py
-------
Solves the Eastvantage Data Engineer assignment.

Two approaches are provided:
  1. SQL-only  → single aggregation query, no Python-side data processing
  2. Pandas    → loads raw tables into DataFrames, processes in Python

Both produce identical output saved as a semicolon-delimited CSV.

Usage
-----
  python main.py              # runs both approaches
  python main.py --sql        # SQL approach only
  python main.py --pandas     # Pandas approach only
"""

import argparse
import logging
import os
import sqlite3

import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH    = "sales.db"
OUTPUT_DIR = "output"
CSV_SQL    = os.path.join(OUTPUT_DIR, "result_sql.csv")
CSV_PANDAS = os.path.join(OUTPUT_DIR, "result_pandas.csv")
DELIMITER  = ";"
AGE_MIN, AGE_MAX = 18, 35

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  APPROACH 1 – Pure SQL
# ══════════════════════════════════════════════════════════════════════════════

SQL_QUERY = """
    SELECT
        c.customer_id   AS Customer,
        c.age           AS Age,
        i.item_name     AS Item,
        CAST(SUM(o.quantity) AS INTEGER) AS Quantity
    FROM   Customer c
    JOIN   Sales    s ON s.customer_id = c.customer_id
    JOIN   Orders   o ON o.sales_id    = s.sales_id
    JOIN   Items    i ON i.item_id     = o.item_id
    WHERE  c.age BETWEEN :age_min AND :age_max
      AND  o.quantity IS NOT NULL
    GROUP  BY c.customer_id, c.age, i.item_id, i.item_name
    HAVING SUM(o.quantity) > 0
    ORDER  BY c.customer_id, i.item_name;
"""


def solve_sql(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return aggregated results using a pure SQL query."""
    logger.info("Running SQL approach...")
    df = pd.read_sql_query(
        SQL_QUERY,
        conn,
        params={"age_min": AGE_MIN, "age_max": AGE_MAX},
    )
    logger.info("SQL approach returned %d rows.", len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  APPROACH 2 – Pandas
# ══════════════════════════════════════════════════════════════════════════════

def solve_pandas(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return aggregated results using Pandas merge / groupby operations."""
    logger.info("Running Pandas approach...")

    # 1. Load raw tables
    customers = pd.read_sql_query("SELECT * FROM Customer", conn)
    sales     = pd.read_sql_query("SELECT * FROM Sales",    conn)
    orders    = pd.read_sql_query("SELECT * FROM Orders",   conn)
    items     = pd.read_sql_query("SELECT * FROM Items",    conn)
    logger.debug(
        "Loaded rows — customers: %d, sales: %d, orders: %d, items: %d",
        len(customers), len(sales), len(orders), len(items),
    )

    # 2. Filter customers to target age range
    customers = customers[customers["age"].between(AGE_MIN, AGE_MAX)]
    logger.debug("%d customers in age range %d–%d.", len(customers), AGE_MIN, AGE_MAX)

    # 3. Join: Customer → Sales → Orders → Items
    df = (
        customers
        .merge(sales,  on="customer_id")
        .merge(orders, on="sales_id")
        .merge(items,  on="item_id")
    )

    # 4. Drop rows where item was not purchased (quantity IS NULL)
    df = df.dropna(subset=["quantity"])

    # 5. Aggregate: total quantity per (customer, age, item)
    df = (
        df
        .groupby(["customer_id", "age", "item_name"], as_index=False)
        ["quantity"]
        .sum()
    )

    # 6. Omit items with zero total purchases
    df = df[df["quantity"] > 0]

    # 7. Cast to integer (no decimals allowed)
    df["quantity"] = df["quantity"].astype(int)

    # 8. Rename columns to required output format
    df = df.rename(columns={
        "customer_id": "Customer",
        "age":         "Age",
        "item_name":   "Item",
        "quantity":    "Quantity",
    })

    # 9. Sort for deterministic output
    df = df.sort_values(["Customer", "Item"]).reset_index(drop=True)

    logger.info("Pandas approach returned %d rows.", len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════════════════════

def save_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, sep=DELIMITER, index=False)
    logger.info("Saved output → %s", path)


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Eastvantage Data Engineer Assignment")
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--sql",    action="store_true", help="Run SQL approach only")
    group.add_argument("--pandas", action="store_true", help="Run Pandas approach only")
    args = parser.parse_args()

    run_sql    = args.sql    or not args.pandas
    run_pandas = args.pandas or not args.sql

    if not os.path.exists(DB_PATH):
        logger.error("Database '%s' not found. Run setup_db.py first.", DB_PATH)
        return

    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as exc:
        logger.error("Could not open database '%s': %s", DB_PATH, exc)
        raise

    try:
        df_sql = df_pd = None

        if run_sql:
            df_sql = solve_sql(conn)
            save_csv(df_sql, CSV_SQL)

        if run_pandas:
            df_pd = solve_pandas(conn)
            save_csv(df_pd, CSV_PANDAS)

        if run_sql and run_pandas and df_sql is not None and df_pd is not None:
            match = df_sql.reset_index(drop=True).equals(df_pd.reset_index(drop=True))
            if match:
                logger.info("Validation passed: both approaches produce identical output.")
            else:
                logger.warning("Validation FAILED: approaches returned different results.")

    except Exception as exc:
        logger.error("Unexpected error during processing: %s", exc)
        raise
    finally:
        conn.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    main()
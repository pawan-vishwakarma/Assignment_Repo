"""
Solves the Eastvantage Data Engineer assignment.

Two approaches are provided:
  1. SQL-only  → uses a single aggregation query, no Python-side processing
  2. Pandas    → loads raw tables into DataFrames, processes in Python

Both approaches produce identical output and write to the same CSV file.

Usage
-----
  python solution.py              # runs both approaches; saves output/result_sql.csv
                                  #   and output/result_pandas.csv
  python solution.py --sql        # SQL approach only
  python solution.py --pandas     # Pandas approach only
"""

import argparse
import os
import sqlite3

import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH     = "sales.db"
OUTPUT_DIR  = "output"
CSV_SQL     = os.path.join(OUTPUT_DIR, "result_sql.csv")
CSV_PANDAS  = os.path.join(OUTPUT_DIR, "result_pandas.csv")
DELIMITER   = ";"
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
      AND  o.quantity IS NOT NULL          -- exclude items not bought
    GROUP  BY c.customer_id, c.age, i.item_id, i.item_name
    HAVING SUM(o.quantity) > 0             -- omit rows whose total = 0
    ORDER  BY c.customer_id, i.item_name;
"""


def solve_sql(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return results using a pure SQL aggregation query."""
    df = pd.read_sql_query(
        SQL_QUERY,
        conn,
        params={"age_min": AGE_MIN, "age_max": AGE_MAX},
    )
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  APPROACH 2 – Pandas
# ══════════════════════════════════════════════════════════════════════════════

def solve_pandas(conn: sqlite3.Connection) -> pd.DataFrame:
    """Return results using Pandas merge / groupby operations."""

    # 1. Load raw tables -------------------------------------------------
    customers = pd.read_sql_query("SELECT * FROM Customer", conn)
    sales     = pd.read_sql_query("SELECT * FROM Sales",    conn)
    orders    = pd.read_sql_query("SELECT * FROM Orders",   conn)
    items     = pd.read_sql_query("SELECT * FROM Items",    conn)

    # 2. Filter customers to target age range ----------------------------
    customers = customers[
        customers["age"].between(AGE_MIN, AGE_MAX)
    ]

    # 3. Join: Customer → Sales → Orders → Items ------------------------
    df = (
        customers
        .merge(sales,  on="customer_id")
        .merge(orders, on="sales_id")
        .merge(items,  on="item_id")
    )

    # 4. Drop rows where item was not purchased (quantity IS NULL) -------
    df = df.dropna(subset=["quantity"])

    # 5. Aggregate: total quantity per (customer, age, item) -------------
    df = (
        df
        .groupby(["customer_id", "age", "item_name"], as_index=False)
        ["quantity"]
        .sum()
    )

    # 6. Omit items with zero total purchases ----------------------------
    df = df[df["quantity"] > 0]

    # 7. Cast to integer (no decimals) -----------------------------------
    df["quantity"] = df["quantity"].astype(int)

    # 8. Rename columns to match required output format ------------------
    df = df.rename(columns={
        "customer_id": "Customer",
        "age":         "Age",
        "item_name":   "Item",
        "quantity":    "Quantity",
    })

    # 9. Sort for deterministic output -----------------------------------
    df = df.sort_values(["Customer", "Item"]).reset_index(drop=True)

    return df


# ══════════════════════════════════════════════════════════════════════════════
#  Output helper
# ══════════════════════════════════════════════════════════════════════════════

def save_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, sep=DELIMITER, index=False)
    print(f"  Saved → {path}")


def display(df: pd.DataFrame, label: str) -> None:
    print(f"\n{'─'*50}")
    print(f"  {label} results  ({len(df)} rows)")
    print(f"{'─'*50}")
    print(df.to_string(index=False))


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
        print(f"[ERROR] Database '{DB_PATH}' not found. Run setup_db.py first.")
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        if run_sql:
            print("\n[Approach 1] Pure SQL")
            df_sql = solve_sql(conn)
            display(df_sql, "SQL")
            save_csv(df_sql, CSV_SQL)

        if run_pandas:
            print("\n[Approach 2] Pandas")
            df_pd = solve_pandas(conn)
            display(df_pd, "Pandas")
            save_csv(df_pd, CSV_PANDAS)

        if run_sql and run_pandas:
            match = df_sql.reset_index(drop=True).equals(
                df_pd.reset_index(drop=True)
            )
            print(f"\n[Validation] Both approaches produce identical output: {match}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

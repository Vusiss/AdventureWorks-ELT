"""
Bridge script: reads all tables from MSSQL aw-db.Extract and writes them
into aw-olap.duckdb (schema: extract).  Run this once before `dbt run`.
"""
import pyodbc
import duckdb
import pandas as pd

MSSQL_CONN = (
    "Driver=ODBC Driver 18 for SQL Server;"
    "Server=127.0.0.1,1433;"
    "Database=aw-db;"
    "Uid=sa;"
    "Pwd=qwerty;"
    "TrustServerCertificate=yes;"
    "Encrypt=no"
)

DUCKDB_PATH = "/home/vusis/University/BI/aw-olap.duckdb"

# Tables to copy (DLT-normalised names in aw-db.Extract)
TABLES = [
    "product",
    "product_subcategory",
    "product_category",
    "sales_person",
    "sales_territory",
    "sales_order_header",
    "sales_order_detail",
    "person",
    "country_region",
    "product_rating",
    "currency_rate_data",
]


def load_table(mssql_cur: pyodbc.Cursor, duck: duckdb.DuckDBPyConnection, table: str):
    print(f"  Copying Extract.{table} ...", end=" ", flush=True)
    mssql_cur.execute(f"SELECT * FROM [Extract].[{table}]")
    cols = [desc[0] for desc in mssql_cur.description]
    rows = mssql_cur.fetchall()

    df = pd.DataFrame([list(r) for r in rows], columns=cols)
    duck.execute(f"DROP TABLE IF EXISTS extract.{table}")
    duck.execute(f"CREATE TABLE extract.{table} AS SELECT * FROM df")
    print(f"{len(df):,} rows")


def main():
    print("Connecting to MSSQL aw-db ...")
    mssql = pyodbc.connect(MSSQL_CONN)
    cur = mssql.cursor()

    print(f"Opening DuckDB: {DUCKDB_PATH}")
    duck = duckdb.connect(DUCKDB_PATH)
    duck.execute("CREATE SCHEMA IF NOT EXISTS extract")

    for table in TABLES:
        try:
            load_table(cur, duck, table)
        except Exception as exc:
            print(f"  SKIPPED ({exc})")

    duck.close()
    mssql.close()
    print("Done.")


if __name__ == "__main__":
    main()

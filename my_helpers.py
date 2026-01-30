import sqlite3
from typing import List, Optional

import pandas as pd


def load_table_as_df(
    db_file: str,
    table_name: str,
    columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Load a table (or a subset of its columns) from an SQLite database into a pandas DataFrame.

    Parameters
    ----------
    db_file : str
        Path to the SQLite database file.  Example: "my_database.sqlite".
    table_name : str
        Name of the table you want to read.
    columns : list[str] | None, optional
        List of column names to fetch.  If ``None`` (the default) all columns are returned.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the requested data.

    Raises
    ------
    FileNotFoundError
        If the database file does not exist.
    sqlite3.Error
        If a problem occurs while connecting to or querying the database.
    ValueError
        If the table name is empty or contains only whitespace.
    """
    if not table_name or table_name.strip() == "":
        raise ValueError("table_name must be a non‑empty string")
    # Build the SELECT clause
    col_expr = "*" if columns is None else ", ".join([f'"{c}"' for c in columns])
    sql = f'SELECT {col_expr} FROM "{table_name}"'
    try:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(sql, conn)
    finally:
        # Ensure the connection is always closed
        conn.close()
    return df

def get_all_table_names(db_file: str) -> List[str]:
    """
    Return a list of all table names in the given SQLite database.

    Parameters
    ----------
    db_file : str
        Path to the SQLite database file.

    Returns
    -------
    List[str]
        Names of every table defined in the database (excluding internal tables such as
        sqlite_sequence).  The order is the same as returned by the query.

    Raises
    ------
    FileNotFoundError
        If the specified database file does not exist.
    sqlite3.Error
        If a problem occurs while connecting to or querying the database.
    """
    # SQLite keeps metadata in the table called "sqlite_master".
    sql = (
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )

    with sqlite3.connect(db_file) as conn:
        cursor = conn.execute(sql)
        tables = [row[0] for row in cursor.fetchall()]

    return tables

import sqlite3

def fetch_table_value_as_int(
    db_path: str,
    table: str,
    search_column: str,
    result_column: str,
    search_value
) -> int | None:
    """
    Fetch a single integer value from SQLite DB.

    Returns:
        int value if found
        None if not found or NULL
    """

    query = f"""
        SELECT {result_column}
        FROM {table}
        WHERE {search_column} = ?
        LIMIT 1
    """

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (search_value,))
            row = cursor.fetchone()

            if row is None or row[0] is None:
                return None

            return int(row[0])

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None

#tables = get_all_table_names("market_feed.db")

#for table_name in enumerate(tables):
#   print(f"{table_name}")


#df_subset = load_table_as_df("market_feed.db", "main_table", ["symbol", "yfinance",])
#print(df_subset.head(10))

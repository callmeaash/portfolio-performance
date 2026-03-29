import duckdb

DB_PATH = '../portfolio.duckdb'


def query(sql: str):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).df()
    con.close()
    return df
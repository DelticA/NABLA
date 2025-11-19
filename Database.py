import duckdb
print(duckdb.__version__)

con = duckdb.connect(":memory:")
print(con.execute("SELECT 42").fetchall())

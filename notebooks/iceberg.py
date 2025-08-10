import marimo

__generated_with = "0.14.16"
app = marimo.App()


@app.cell
def _():
    from pyiceberg.catalog import load_catalog
    from pathlib import Path
    import pyarrow.parquet as pq

    warehouse_path = Path("./warehouse").absolute()

    catalog = load_catalog(
        "default",
        **{
            'type': 'sql',
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    return catalog, pq


@app.cell
def _(pq):
    df = pq.read_table("yellow_tripdata_2023-01.parquet")
    return (df,)


@app.cell
def _(catalog, df):
    catalog.create_namespace("default")

    table = catalog.create_table(
        "default.taxi_dataset",
        schema=df.schema,
    )
    return


@app.cell
def _(catalog):
    table_1 = catalog.load_table('default.taxi_dataset')
    return (table_1,)


@app.cell
def _(df, table_1):
    table_1.append(df)
    return


@app.cell
def _():
    import sqlite3
    con = sqlite3.connect('./warehouse/pyiceberg_catalog.db')
    cur = con.cursor()
    res = cur.execute('SELECT name FROM sqlite_master')
    res.fetchone()
    return (cur,)


@app.cell
def _(cur):
    res = cur.execute('SELECT * FROM iceberg_tables')
    res.fetchall()
    return


if __name__ == "__main__":
    app.run()

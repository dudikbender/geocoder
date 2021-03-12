import sqlite3
from sqlite3 import Error
from pathlib import Path

db_file = Path("../geocoder/app/database.db")

def dict_factory(cursor, row):
    d = {}
    for idx,col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

connection = sqlite3.connect(db_file)
connection.row_factory = dict_factory

def print_version():
    cur = connection.cursor()
    cur.execute('SELECT SQLITE_VERSION()')
    data = cur.fetchone()[0]

    print(f"SQLite version: {data}")
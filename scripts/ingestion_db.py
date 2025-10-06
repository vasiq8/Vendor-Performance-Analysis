import os
import pandas as pd
from sqlalchemy import create_engine
import sqlite3

if os.path.exists('inventory.db'):
    os.remove('inventory.db')

engine = create_engine('sqlite:///inventory.db')

csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
csv_files.sort(key=lambda x: os.path.getsize(x))

for file in csv_files:
    df = pd.read_csv(file)
    table_name = file[:-4]
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

with sqlite3.connect('inventory.db') as conn:
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
    print("Tables in database:")
    for table in tables['name']:
        count = pd.read_sql_query(f"SELECT COUNT(*) as c FROM {table}", conn)['c'][0]
        print(f"{table}: {count} rows")
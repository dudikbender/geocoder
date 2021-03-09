from utils.db import connection, print_version
import pandas as pd

def add_table(csv_file, table_name, engine):
    df = pd.read_csv(csv_file)
    df = df.drop('Unnamed: 0')
    df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')

table = 'data/tables/postcode_coordinates.csv'

add_table(table, 'Postcode_coordinates', connection)

cur = connection.cursor()
cur.execute('''SELECT *
            FROM Postcode_coordinates''')

data = cur.fetchmany(5)

print(data)
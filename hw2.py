import pandas as pd
import sqlite3
# pd.read_csv("/Users/arusharamanathan/databases")
df = pd.read_csv("salary_data.csv")

print(df.head())
print(df.tail())

connection = sqlite3.connect("salaries.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS salaries")
df.to_sql("salaries", connection)
connection.close()
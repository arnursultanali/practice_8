import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="altynbekamankozha",
    user="altynbekamankozha",
    password=""
)

cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())

cur.close()
conn.close()
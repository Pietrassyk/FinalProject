import mysql.connector

conn = mysql.connector.Connect(
    host='debaterdb.c7oenlqovcjd.us-east-2.rds.amazonaws.com',
    user="piet",
    password="philipsWe200",
    database='debater')

c = conn.cursor()
c.execute("show tables")
out = c.fetchall()
print(out)
conn.close()
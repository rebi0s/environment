import psycopg2

#conn = psycopg2.connect(dbname="db_iceberg",
#                        user="icbergcat",
#                        host="127.0.0.1",
#                        password="hNXz35UBRcAC",
#                        port="32368")
#cursor = conn.cursor()
#cursor.execute('SELECT * FROM iceberg_tables')
#rows = cursor.fetchall()
#for table in rows:
#    print(table)
#conn.close()

conn = psycopg2.connect(dbname="db_iceberg",
                        user="role_iceberg",
                        host="34.228.176.207",
                        password="hNXz35UBRcAC",
                        port="5432")
cursor = conn.cursor()
cursor.execute('SELECT *  from iceberg_tables')
rows = cursor.fetchall()
for table in rows:
    print(table)
conn.close()


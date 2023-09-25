import psycopg2
import os
import stats_pre_processor
import datetime

print("Start daily report task.")

conn_str = f"dbname={os.getenv('DB_NAME')}"

if "DB_HOST" in os.environ:
    conn_str += " host={} ".format(os.environ['DB_HOST'])
if "DB_USER" in os.environ:
    conn_str += " user={}".format(os.environ['DB_USER'])
if "DB_PASSWORD" in os.environ:
    conn_str += " password={}".format(os.environ['DB_PASSWORD'])
if "DB_PORT" in os.environ:
    conn_str += " port={}".format(os.environ['DB_PORT'])

conn = psycopg2.connect(conn_str)

stats_pre_processor.pre_proccess(conn)

cur = conn.cursor()

print("Query all municipalities with at least 1 vehicle.")
cur.execute("""SELECT name, municipality
    FROM zones 
    WHERE zone_type = 'municipality'
    AND ST_intersects((SELECT st_collect(location)
    FROM park_events
    WHERE start_time < NOW()
    AND end_time is null), area);
""")

municipalities = cur.fetchall()

print("Delete all data in municipalities_with_data.")
cur.execute("DELETE FROM municipalities_with_data")
print("Insert municipalities.")
for municipality in municipalities:
    cur.execute("""
        INSERT INTO municipalities_with_data (name, municipality)
        VALUES (%s, %s)
    """, (municipality[0], municipality[1]))
conn.commit()
conn.close()

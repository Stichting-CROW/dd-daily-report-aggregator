import psycopg2
import os
import stats_pre_processor
from datetime import datetime, timedelta, timezone

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO is common for general use)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Include timestamp, level, and message
    datefmt="%Y-%m-%d %H:%M:%S",  # Format the timestamp
)

logging.info("Start daily report task.")

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

logging.info("Query all municipalities with at least 1 vehicle.")
cur.execute("""SELECT name, municipality
    FROM zones 
    WHERE zone_type = 'municipality'
    AND ST_intersects((SELECT st_collect(location)
    FROM park_events
    WHERE start_time < NOW()
    AND end_time is null), area);
""")

municipalities = cur.fetchall()

logging.info("Delete all data in municipalities_with_data.")
cur.execute("DELETE FROM municipalities_with_data")
logging.info("Insert municipalities.")
for municipality in municipalities:
    cur.execute("""
        INSERT INTO municipalities_with_data (name, municipality)
        VALUES (%s, %s)
    """, (municipality[0], municipality[1]))

def update_materialized_views(cur):
    logging.info("Start refresh materialized view park_event_on_date")
    cur.execute("REFRESH MATERIALIZED VIEW park_event_on_date")
    logging.info("Finished refresh materialized view park_event_on_date")
    
    logging.info("DROP INDEX IF EXISTS park_events_ended_less_than_three_days_ago")
    cur.execute("DROP INDEX IF EXISTS idx_park_events_location_recent_gist;")
    logging.info("Finished DROP INDEX idx_park_events_location_recent_gist")
    
    logging.info("DROP INDEX IF EXISTS park_events_ended_less_than_three_days_ago") 
    cur.execute("DROP INDEX IF EXISTS park_events_ended_less_than_three_days_ago;")
    logging.info("Finished DROP INDEX park_events_ended_less_than_three_days_ago")


    date_three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3))
    logging.info("Create new index on park_events starting from_date: %s", date_three_days_ago)
    stmt = """
        CREATE INDEX park_events_ended_less_than_three_days_ago
        ON park_events (end_time)
        WHERE end_time >= %s OR end_time IS NULL;
    """
    cur.execute(stmt, (date_three_days_ago,))

    stmt = """
        CREATE INDEX IF NOT EXISTS idx_park_events_location_recent_gist
        ON park_events
        USING gist (location)
        WHERE end_time >= %s OR end_time IS NULL;
    """

    cur.execute(stmt, (date_three_days_ago,))
    logging.info("Finished creating index")
   

update_materialized_views(cur)


conn.commit()
conn.close()

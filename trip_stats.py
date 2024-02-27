import pre_stat


class NumberOfTripsStats():
    def __init__(self):
        self.number_of_trips_started_stat = pre_stat.Stat("number_of_trip_started")

    def retreive_dates_for_calculating_stats(self, conn, number_of_dates_in_the_past):
        stmt = """
            SELECT q1.date 
            FROM
                (SELECT date_trunc('day', dd)::date as date
                FROM generate_series
                ( now()::date - %s, now()::date - 1, '1 day'::interval) dd) as q1
            WHERE q1.date NOT IN (SELECT date 
                FROM stats_pre_process
                WHERE stat_description = 'number_of_trip_started');
        """
        cur = conn.cursor()
        cur.execute(stmt, (number_of_dates_in_the_past,))
        return [record[0] for record in cur.fetchall()]

    def pre_process_stats(self, conn, number_of_days_in_the_past):
        date_list = self.retreive_dates_for_calculating_stats(conn, number_of_days_in_the_past)
        for datestamp in date_list:
            print("Calculate date " + str(datestamp))
            trips_started_on_date = self.get_trips_made_on_date(conn, datestamp)
            self.derive_stats(trips_started_on_date)
            self.store_stats(conn, datestamp)

    def get_trips_made_on_date(self, conn, date):
        cur = conn.cursor()
        stmt = """SELECT trip_id,
            (SELECT array_agg(stats_ref) from zones where st_intersects(start_location, area) AND stats_ref IS NOT null), system_id
            FROM trips
            WHERE start_time >= %s and start_time <= %s + '1 day'::interval;  """
        cur.execute(stmt, (date, date, ))
        return cur.fetchall()

    def derive_stats(self, data_trips_started_on_date):
        self.reset_stats()
        for record in data_trips_started_on_date:
            stat_refs = record[1]
            system_id = record[2]
            if not stat_refs:
                continue
            for stat_ref in stat_refs:
                self.number_of_trips_started_stat.increment(stat_ref, system_id)

    def reset_stats(self):
         self.number_of_trips_started_stat.reset()

    def store_stats(self, conn, datestamp):
        self.number_of_trips_started_stat.store(conn, datestamp)


# This class could be splitted in two seperate classes.
class TripDurationStats():
    def __init__(self):
        self.number_of_trips_ended_stat = pre_stat.Stat("number_of_trips_ended")
        self.trip_duration = pre_stat.Stat("sum_of_trip_duration")

    def retreive_dates_for_calculating_stats(self, conn, number_of_dates_in_the_past):
        stmt = """
            SELECT q1.date 
            FROM
                (SELECT date_trunc('day', dd)::date as date
                FROM generate_series
                ( now()::date - %s, now()::date - 1, '1 day'::interval) dd) as q1
            WHERE q1.date NOT IN (SELECT date 
                FROM stats_pre_process
                WHERE stat_description = 'number_of_trips_ended');
        """
        cur = conn.cursor()
        cur.execute(stmt, (number_of_dates_in_the_past,))
        return [record[0] for record in cur.fetchall()]
        
    def pre_process_stats(self, conn, number_of_days_in_the_past):
        date_list = self.retreive_dates_for_calculating_stats(conn, number_of_days_in_the_past)
        for datestamp in date_list:
            print("Calculate date " + str(datestamp))
            trips_ended_on_date = self.get_trips_ended_on_date(conn, datestamp)
            self.derive_stats(trips_ended_on_date)
            self.store_stats(conn, datestamp)

    def derive_stats(self, data_trips_ended_on_date):
        self.reset_stats()
        for record in data_trips_ended_on_date:
            stat_refs = record[1]
            system_id = record[2]
            if not stat_refs:
                continue
            self.derive_trip_duration_stat(stat_refs, system_id, record[0].total_seconds())   

    def derive_trip_duration_stat(self, stat_refs, system_id, duration):
        for stat_ref in stat_refs:
            self.number_of_trips_ended_stat.increment(stat_ref, system_id)
            self.trip_duration.add(stat_ref, system_id, duration)

    def reset_stats(self):
        self.number_of_trips_ended_stat.reset()
        self.trip_duration.reset()

    def store_stats(self, conn, datestamp):
        self.number_of_trips_ended_stat.store(conn, datestamp)
        self.trip_duration.store(conn, datestamp)

    # Trips die vandaag afgesloten zijn.
    def get_trips_ended_on_date(self, conn, date):
        cur = conn.cursor()
        stmt = """SELECT (end_time - start_time) as trip_duration,
            (SELECT array_agg(stats_ref) from zones where st_intersects(start_location, area) and stats_ref IS NOT NULL), system_id
            FROM trips
            WHERE end_time >= %s and end_time <= %s + '1 day'::interval;  """
        cur.execute(stmt, (date, date,))
        return cur.fetchall()


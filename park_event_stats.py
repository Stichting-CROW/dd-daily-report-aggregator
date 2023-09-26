import pre_stat
import datetime

class ParkEventStats():
    def __init__(self, park_event_stat_list):
        self.park_event_stat_list = park_event_stat_list

    def retreive_dates_for_calculating_stats(self, conn, number_dates_in_the_past):
        stmt = """
            SELECT q1.date::timestamp
            FROM
                (SELECT date_trunc('day', dd)::date as date
                FROM generate_series
                ( now()::date - %s, now()::date, '1 day'::interval) dd) as q1
            WHERE q1.date NOT IN (SELECT date 
                FROM stats_pre_process
                WHERE stat_description = 'number_of_vehicles_available');
        """
        cur = conn.cursor()
        cur.execute(stmt, (number_dates_in_the_past,))
        return [record[0] for record in cur.fetchall()]

    def pre_process_stats(self, conn, number_of_days_in_the_past):
        date_list = self.retreive_dates_for_calculating_stats(conn, number_of_days_in_the_past)
        for datestamp in date_list:
            print("Calculate date " + str(datestamp))
            raw_stats = self.get_available_vehicles_on_date(conn, datestamp)
            self.derive_stats(raw_stats)
            self.store_stats(conn, datestamp)

    def derive_stats(self, data):
        self.reset_stats()
        frequency = {}
        for record in data:
            if not record[1]:
                continue
            for stat_ref in record[1]:
                self.derive_stat_per_criterium(stat_ref, record[2], record[0])

    def reset_stats(self):
        for park_event_stat in self.park_event_stat_list:
            park_event_stat.stat.get_value()
            park_event_stat.stat.reset()

    def derive_stat_per_criterium(self, stat_ref, system_id, duration):
        for park_event_stat in self.park_event_stat_list:
            if not park_event_stat.check_criterium(duration):
                return
            park_event_stat.stat.increment(stat_ref, system_id)

    def store_stats(self, conn, datestamp):
        for park_event_stat in self.park_event_stat_list:
            park_event_stat.stat.store(conn, datestamp)
           
    def get_available_vehicles_on_date(self, conn, date):
        cur = conn.cursor()
        date.replace(hour=3, minute=0, second=0)
        stmt = """SELECT (%s - start_time) as park_duration_at_this_moment,
            (SELECT array_agg(stats_ref) from zones where st_intersects(location, area) and stats_ref IS NOT NULL), system_id
            FROM park_events
            WHERE start_time <= %s
            AND (end_time > %s OR end_time IS NULL)"""
        cur.execute(stmt, (date, date, date, ))
        return cur.fetchall()


class ParkEventStat():
    def __init__(self, name, duration):
        self.name = name
        self.duration_criteria = duration
        self.stat = pre_stat.Stat(name)

    def check_criterium(self, park_event_duration):
        return park_event_duration.total_seconds() > self.duration_criteria

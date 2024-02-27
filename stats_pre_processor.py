import park_event_stats
import trip_stats
import report_stat_collector

def pre_proccess(conn):
    number_of_days_in_past = 550
    park_event_stat_list = []
    park_event_stat_list.append(park_event_stats.ParkEventStat("number_of_vehicles_available", 0))
    park_event_stat_list.append(park_event_stats.ParkEventStat("number_of_vehicles_available_longer_then_24_hours", 3600*24*1))
    park_event_stat_list.append(park_event_stats.ParkEventStat("number_of_vehicles_available_longer_then_4_days", 3600*24*4))
    park_event_stat_list.append(park_event_stats.ParkEventStat("number_of_vehicles_available_longer_then_7_days", 3600*24*7))

    print("Calculate number_of_trips_stats")
    number_of_trips_stats = trip_stats.NumberOfTripsStats()
    number_of_trips_stats.pre_process_stats(conn, number_of_days_in_past)

    print("Calculate duration stats.")
    trip_duration_stat = trip_stats.TripDurationStats()
    trip_duration_stat.pre_process_stats(conn, number_of_days_in_past)    

    print("Calculcate park_event_stats")
    park_event_stats_deriver = park_event_stats.ParkEventStats(park_event_stat_list)
    park_event_stats_deriver.pre_process_stats(conn, number_of_days_in_past)

   


    
"""
pre_calculate:
number_of_vehicles_available
number_of_rentals_yesterday

on_the_fly
number_of_rentals_per_vehicle_yesterday

pre_calculate
number_of_vehicles_available_longer_then_24_hours

on_the_fly
percentage_of_vehicles_available_longer_then_24_hours

pre_calculate
number_of_vehicles_available_longer_then_4_days

on_the_fly
percentage_of_vehicles_available_longer_then_4_days

pre_calculate
number_of_vehicles_available_longer_then_7_days

on_the_fly
percentage_of_vehicles_available_longer_then_7_days

"""
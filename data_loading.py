import pandas as pd
import datetime


def load_data(data_to_load):
    return pd.DataFrame.from_csv('google_transit/' + data_to_load + '.txt')


def load_stops():
    stops_all = pd.DataFrame.from_csv('google_transit/stops.txt')
    return stops_all.dropna(how='all', subset=['stop_code', 'parent_station'])

def load_stop_times():
    return load_data('stop_times')

def load_stop_times_today(stop_times, trips_today):
    return stop_times[stop_times.index.isin(trips_today.trip_id)]

def load_stop_times_tomorrow(stop_times, trips_tomorrow):
    return stop_times[stop_times.index.isin(trips_tomorrow.trip_id)]

def load_trips():
    return load_data('trips')

def load_trips_today(trips, calendar_today):
    return trips[trips.service_id.isin(calendar_today.index.values)]

def load_trips_tomorrow(trips, calendar_tomorrow):
    return trips[trips.service_id.isin(calendar_tomorrow.index.values)]

def load_calendar():
    return load_data('calendar')

def load_calendar_today(calendar, cur_datetime):
    date_today = int(cur_datetime.strftime('%Y%m%d'))
    weekday_today = cur_datetime.strftime('%A').lower()
    calendar_today = calendar[(calendar.start_date < date_today)
                              & (calendar.end_date > date_today)
                              & (calendar[weekday_today] == 1)]
    return calendar_today

def load_calendar_tomorrow(calendar, cur_datetime):
    date_tomorrow = int((cur_datetime + datetime.timedelta(days=1)).strftime('%Y%m%d'))
    weekday_tomorrow = (cur_datetime + datetime.timedelta(days=1)).strftime('%A').lower()
    calendar_tomorrow = calendar[(calendar.start_date < date_tomorrow)
                                 & (calendar.end_date > date_tomorrow)
                                 & (calendar[weekday_tomorrow] == 1)]
    return calendar_tomorrow

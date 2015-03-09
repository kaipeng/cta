import datetime

import pandas as pd
import datetime


DATETIME_COLUMNS = ['arrival_time']
STOP_TIMES_COLUMNS_DROP = ['departure_time', 'pickup_type', 'shape_dist_traveled']
TRIPS_COLUMNS_DROP = ['schd_trip_id', 'block_id', 'direction_id', 'wheelchair_accessible']
STOPS_COLUMNS_DROP = ['location_type', 'stop_code']



def load_data(data_to_load):
    data = pd.DataFrame.from_csv('google_transit/' + data_to_load + '.txt')
    #return convert_time_strings(data)
    return data


def convert_time_strings(raw_data):
    for datetime_column in DATETIME_COLUMNS:
        if datetime_column in raw_data.columns:
            datetime_data = raw_data[datetime_column]
            raw_data[datetime_column + '_datetime'] = [convert_time_string(time_string) for time_string in
                                                       datetime_data]
    return raw_data


def convert_time_string(datetime_string):
    time_elements = datetime_string.split(':')
    hour = int(time_elements[0])
    current_datetime = datetime.datetime.today()
    current_hour = current_datetime.hour
    datetime_today = datetime.datetime(current_datetime.year, current_datetime.month, current_datetime.day,
                                       hour % 24, int(time_elements[1]), int(time_elements[2]))
    if hour < current_hour or hour > 23:
        return datetime_today + datetime.timedelta(days=1)
    else:
        return datetime_today


def load_stops():
    stops_all = pd.DataFrame.from_csv('google_transit/stops.txt')
    stops_all = stops_all.dropna(how='all', subset=['stop_code', 'parent_station'])
    for drop in STOPS_COLUMNS_DROP:
        del stops_all[drop]
    return stops_all

def load_stop_times():
    stop_times = load_data('stop_times')
    for datetime_column in DATETIME_COLUMNS:
        stop_times[datetime_column + '_24'] = [(str(int(time[0:2])+24) + time[2:])
                                               for time in stop_times[datetime_column]]
    for drop in STOP_TIMES_COLUMNS_DROP:
        del stop_times[drop]
    return stop_times

def load_stop_times_date(stop_times, trips):
    return stop_times[stop_times.index.isin(trips.trip_id)]

def load_trips():
    trips = load_data('trips')
    for drop in TRIPS_COLUMNS_DROP:
        del trips[drop]
    return trips

def load_trips_date(trips, calendar):
    return trips[trips.service_id.isin(calendar.index.values)]

def load_calendar():
    return load_data('calendar')

def load_calendar_date(calendar, start_date, days_delta=0):
    load_date = start_date + datetime.timedelta(days=days_delta)
    date_dt = int(load_date.strftime('%Y%m%d'))
    weekday_dt = load_date.strftime('%A').lower()
    calendar_dt = calendar[(calendar.start_date < date_dt)
                           & (calendar.end_date > date_dt)
                           & (calendar[weekday_dt] == 1)]
    return calendar_dt


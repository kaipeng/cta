from threading import Timer

import tornado.ioloop
import tornado.web
from tornado.options import define, options, parse_command_line

from data_loading import load_stops, load_calendar, load_stop_times, load_trips
from data_loading import load_trips_date, load_stop_times_date, load_calendar_date

from fetch_cta_data import fetch_cta_data
from get_nearest_stop import get_nearest_stop

import datetime
import time
import pandas as pd

NUMBER = r'-?[0-9]+\.*[0-9]*'

define("port", default=8888, help="run on the given port", type=int)


class DataManager():
    def __init__(self):
        fetch_cta_data()
        print "Parsing Data into memory"
        self.stops = load_stops()
        self.calendar = load_calendar()
        self.stop_times = load_stop_times()
        self.trips = load_trips()
        self.trips_window = None
        self.stop_times_window = None
        self.stop_times_window_end_datetime = None
        self.stop_times_window_start_datetime = None
        print "Finished Parsing"

    def load_stop_times_in_window(self, start_datetime, window_in_hours):
        #start = time.time()
        calendar_today = load_calendar_date(self.calendar, start_datetime, days_delta=0)
        trips_today = load_trips_date(self.trips, calendar_today)
        stop_times_today = load_stop_times_date(self.stop_times, trips_today)
        start_time_string = start_datetime.strftime('%H:%M:%S')
        end_datetime = start_datetime + datetime.timedelta(hours=window_in_hours)
        end_time_string = end_datetime.strftime('%H:%M:%S')

        # case where window crosses into next day (maybe new schedule)
        if start_datetime.date() < end_datetime.date():
            stop_times_window = stop_times_today[(stop_times_today.arrival_time > start_time_string)]

            calendar_tomorrow = load_calendar_date(self.calendar, start_datetime, days_delta=0)
            trips_tomorrow = load_trips_date(self.trips, calendar_tomorrow)
            stop_times_tomorrow = load_stop_times_date(self.stop_times, trips_tomorrow)
            stop_times_window_tomorrow = stop_times_tomorrow[(stop_times_tomorrow.arrival_time < end_time_string)]

            stop_times_window_tomorrow['arrival_time'] = stop_times_window_tomorrow['arrival_time_24']
            stop_times_window_tomorrow['departure_time'] = stop_times_window_tomorrow['departure_time_24']

            stop_times_window = pd.concat(stop_times_window, stop_times_window_tomorrow)

        else:
            stop_times_window = stop_times_today[(stop_times_today.arrival_time > start_time_string)
                                                 & (stop_times_today.arrival_time < end_time_string)]

        #end = time.time()
        #print end - start
        return stop_times_window

    def get_nearest_stops(self, lat, lon, num=10):
        my_stops = self.stops.copy()
        my_stops['my_lat'] = lat
        my_stops['my_lon'] = lon
        my_stops['dist'] = ((my_stops.my_lat-my_stops.stop_lat)**2
                            + (my_stops.my_lon-my_stops.stop_lon)**2)**.5 * 69
        return my_stops.sort(columns=['dist'])[0:num]

    def get_stop_times(self, stops, start_datetime=None, window_in_hours=1):
        if not start_datetime:
            start_datetime = datetime.datetime.now()

        if (start_datetime + datetime.timedelta(hours=window_in_hours) > self.stop_times_window_end_datetime) or \
                (start_datetime < self.stop_times_window_start_datetime):
            window = self.load_stop_times_in_window(start_datetime, window_in_hours)
        else:
            window = self.stop_times_window

        start_time = start_datetime.time().strftime('%H:%M:%S')
        end_time = (start_datetime+datetime.timedelta(hours=window_in_hours)).time().strftime('%H:%M:%S')
        my_stop_times = window[(window.arrival_time > start_time)
                               & (window.arrival_time < end_time)
                               & (window.stop_id.isin(stops.index))]
        return my_stop_times

    def get_trips(self, stop_times):
        my_trips = self.trips.copy()
        my_trips = my_trips[my_trips.trip_id.isin(stop_times.index)]
        return my_trips

    def get_arrivals(self, stops, stop_times, trips):
        next_arrivals = stop_times.join(trips.reset_index().set_index('trip_id'))
        next_arrivals_stops = next_arrivals.reset_index().set_index('stop_id').join(stops).reset_index()
        next_arrivals_stops = next_arrivals_stops.sort(columns=['dist', 'arrival_time'])
        return next_arrivals_stops


class DefaultHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.set_header("Content-Type", "text/plain")
        self.write("Success!")
        self.finish()


class NearestStopHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, my_lat, my_lon):
        self.set_header("Content-Type", "text/json")
        stops = data_manager.stops
        stop = get_nearest_stop(stops, [float(my_lat), float(my_lon)])
        self.write(stop.to_json(orient='records'))
        self.finish()

class NearbyStopsHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        lat = float(self.get_argument('lat'))
        lon = float(self.get_argument('lon'))
        num_stops = int(self.get_argument('stops', default=10))

        self.set_header("Content-Type", "text/json")
        stops = data_manager.get_nearest_stops(lat, lon, num_stops)
        self.write(stops.to_json(orient='records'))
        self.finish()

class NearbyArrivalsHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        print self.get_argument('lat')
        lat = float(self.get_argument('lat'))
        lon = float(self.get_argument('lon'))
        num_stops = int(self.get_argument('stops', default=10))
        window_in_hours = int(self.get_argument('hours', default=1))
        start_datetime_arg = self.get_argument('start', default=None)

        try:
            start_datetime = pd.to_datetime(start_datetime_arg)
        except:
            start_datetime = None

        self.set_header("Content-Type", "text/json")
        stops = data_manager.get_nearest_stops(lat, lon, num_stops)
        stop_times = data_manager.get_stop_times(stops, start_datetime=start_datetime, window_in_hours=window_in_hours)
        trips = data_manager.get_trips(stop_times)
        arrivals = data_manager.get_arrivals(stops, stop_times, trips)

        self.write(arrivals.to_json(orient='records'))
        self.finish()

def schedule_data_manager_job(delay_in_seconds, job_method):
    Timer(delay_in_seconds, job_method, [delay_in_seconds]).start()


def update_data_manager_job(delay_in_seconds):
    print "updating data manager"
    update_data_manager()
    schedule_data_manager_job(delay_in_seconds, update_data_manager_job)


def update_data_manager():
    new_data_manager = DataManager()
    global data_manager
    data_manager = new_data_manager

def update_stop_times_window_job(delay_in_seconds):
    print "updating stop times window"
    global data_manager
    start_dt = datetime.datetime.now()
    data_manager.stop_times_window = data_manager.load_stop_times_in_window(start_dt, 3)
    data_manager.stop_times_window_start_datetime = start_dt
    data_manager.stop_times_window_end_datetime = start_dt + datetime.timedelta(hours=3)
    print start_dt, data_manager.stop_times_window_end_datetime
    schedule_data_manager_job(delay_in_seconds, update_stop_times_window_job)

settings = {'debug': True}
application = tornado.web.Application([
    (r'/', DefaultHandler),
    (r'/stops?', NearbyStopsHandler),
    (r'/arrivals?', NearbyArrivalsHandler),

    ((r'/stops/(%s)/(%s)' % (NUMBER, NUMBER)), NearestStopHandler),
], **settings)

data_manager = None

if __name__ == '__main__':
    parse_command_line()
    data_manager = DataManager()

    schedule_data_manager_job(60 * 60 * 6, update_data_manager_job)
    update_stop_times_window_job(60 * 60 * 1)

    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

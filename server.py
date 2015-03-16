from threading import Timer

import tornado.ioloop
import tornado.web
from tornado.options import define, options, parse_command_line

from data_loading import load_stops, load_calendar, load_stop_times, load_trips
from data_loading import load_trips_date, load_stop_times_date, load_calendar_date

from fetch_cta_data import fetch_cta_data, check_update_needed
from get_nearest_stop import get_nearest_stop

import datetime
from dateutil import tz
import threading
import pandas as pd

from cta_api import BusAPIHandler, TrainAPIHandler

NUMBER = r'-?[0-9]+\.*[0-9]*'
CENTRAL_TZ = tz.gettz('America/Chicago')

define("port", default=9999, help="run on the given port", type=int)


class DataManager():
    def __init__(self):
        self.data_last_downloaded = {'local_last_update_dt': None,
                                     'cta_last_update_dt': None}
        self.stops = None
        self.trips = None
        self.calendar = None
        self.stop_times = None
        self.trips_window = None
        self.stop_times_window = None
        self.stop_times_window_end_datetime = None
        self.stop_times_window_start_datetime = None
        self.arrival_columns = ['stop_id', 'trip_id', 'route_id',
                                'direction', 'service_id',
                                'arrival_time', 'dist', 'stop_lat', 'stop_lon',
                                'stop_sequence', 'stop_headsign', 'stop_name', 'stop_desc',
                                'wheelchair_boarding', 'shape_id']
                                # 'shape_dist_travelled', 'schd_trip_id', 'block_id']
        print "Initializing Data Manager"
        self.parse_data_into_memory()

    def parse_data_into_memory(self):
        print "Checking for CTA data"
        self.data_last_downloaded = fetch_cta_data()
        print "Parsing Data into memory"
        self.stops = load_stops()
        self.calendar = load_calendar()
        self.stop_times = load_stop_times()
        self.trips = load_trips()
        print "Finished Parsing"

    def load_stop_times_in_window(self, start_datetime, window_in_hours):
        start_time_string = start_datetime.strftime('%H:%M:%S')
        end_datetime = start_datetime + datetime.timedelta(hours=window_in_hours)
        end_time_string = end_datetime.strftime('%H:%M:%S')
        print 'Loading stop times from ', start_time_string, ' thru ', end_time_string

        calendar_today = load_calendar_date(self.calendar, start_datetime, days_delta=0)
        trips_today = load_trips_date(self.trips, calendar_today)
        stop_times_today = load_stop_times_date(self.stop_times, trips_today)

        # case where window crosses into next day (maybe new schedule)
        if start_datetime.date() < end_datetime.date():
            print 'Window crossing over midnight. Creating 2 windows for 2 days.'
            stop_times_window = stop_times_today[(stop_times_today.arrival_time > start_time_string)]

            calendar_tomorrow = load_calendar_date(self.calendar, start_datetime, days_delta=1)
            trips_tomorrow = load_trips_date(self.trips, calendar_tomorrow)
            stop_times_tomorrow = load_stop_times_date(self.stop_times, trips_tomorrow)
            stop_times_window_tomorrow = stop_times_tomorrow[(stop_times_tomorrow.arrival_time < end_time_string)]

            print 'Setting tomorrow arrival_time to arrival_time_24. Concat lengths: ', \
                len(stop_times_window), len(stop_times_window_tomorrow)
            stop_times_window_tomorrow['arrival_time'] = stop_times_window_tomorrow['arrival_time_24']
            stop_times_window = pd.concat([stop_times_window, stop_times_window_tomorrow])

        else:
            stop_times_window = stop_times_today[(stop_times_today.arrival_time > start_time_string)
                                                 & (stop_times_today.arrival_time < end_time_string)]

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
            start_datetime = datetime.datetime.now(CENTRAL_TZ)

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
        # cut down useless columns
        return next_arrivals_stops[self.arrival_columns]


class DefaultHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        global data_manager_init_thread
        if data_manager_init_thread.isAlive():
            self.write("Server Initializing")
            self.finish()
            return
        api_endpoints = [
            "arrivals?lat=41.8991186&lon=-87.629056",
            "arrivals?lat=41.8991186&lon=-87.629056&stops=10&hours=2&start=2015-04-08T14:30:00",
            "stops?lat=41.8991186&lon=-87.629056&stops=10",
        ]
        self.write("<!DOCTYPE html> <html><body><p>" +
                   "Data Status: <br>" +
                   "Data Last Updated by CTA: " + str(data_manager.data_last_downloaded['cta_last_update_dt']) +
                   "<br>" +
                   "Data Last Fetched by Server: " + str(data_manager.data_last_downloaded['local_last_update_dt']) +
                   "<br>" +
                   "Memory-Cached Stop Times Start: " + str(data_manager.stop_times_window_start_datetime) +
                   "<br>" +
                   "Memory-Cached Stop Times End: " + str(data_manager.stop_times_window_end_datetime) +
                   "<br><br><br>" +
                   "API Endpoints: <br>" +
                   ''.join(['<a href=' + url + '>' + url + '</a><br>' for url in api_endpoints]) +
                   "</p></body></html>")
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
        lat = float(self.get_argument('lat'))
        lon = float(self.get_argument('lon'))
        num_stops = int(self.get_argument('stops', default=10))
        window_in_hours = int(self.get_argument('hours', default=1))
        start_datetime_arg = self.get_argument('start', default=None)

        try:
            start_datetime = pd.to_datetime(start_datetime_arg).tz_localize(CENTRAL_TZ)
        except:
            start_datetime = None

        self.set_header("Content-Type", "text/json")
        stops = data_manager.get_nearest_stops(lat, lon, num_stops)
        stop_times = data_manager.get_stop_times(stops, start_datetime=start_datetime, window_in_hours=window_in_hours)
        trips = data_manager.get_trips(stop_times)
        arrivals = data_manager.get_arrivals(stops, stop_times, trips)

        self.write(arrivals.to_json(orient='records'))
        self.finish()

def schedule_job(delay_in_seconds, job_method):
    Timer(delay_in_seconds, job_method, [delay_in_seconds]).start()


def update_data_manager_job(delay_in_seconds):
    # Modified to only update / reload into memory if a new version is available
    print "Checking for data updates"
    need_update, cta_last_update_dt, local_last_update_dt = check_update_needed()

    if need_update:
        print "Update Needed: ", need_update, cta_last_update_dt, local_last_update_dt
        global data_manager_init_thread
        data_manager_init_thread = threading.Thread(target=init_data_manager_daemon)
        data_manager_init_thread.setDaemon(True)
        data_manager_init_thread.start()

    schedule_job(delay_in_seconds, update_data_manager_job)


def update_stop_times_window_job(delay_in_seconds):
    update_stop_times_window()
    schedule_job(delay_in_seconds, update_stop_times_window_job)


def update_stop_times_window():
    try:
        print "Updating Stop Times Window"
        global data_manager
        start_dt = datetime.datetime.now(CENTRAL_TZ)
        data_manager.stop_times_window = data_manager.load_stop_times_in_window(start_dt, 3)
        data_manager.stop_times_window_start_datetime = start_dt
        data_manager.stop_times_window_end_datetime = start_dt + datetime.timedelta(hours=3)
        print "Stop Times Window Updated: ", start_dt, data_manager.stop_times_window_end_datetime
    except Exception as e:
        print e

def init_data_manager_daemon():
    try:
        global data_manager
        data_manager = DataManager()
        update_stop_times_window()
    except Exception as e:
        print e

settings = {'debug': True}
application = tornado.web.Application([
    (r'/', DefaultHandler),
    (r'/stops?', NearbyStopsHandler),
    (r'/arrivals?', NearbyArrivalsHandler),
    (r'/bus/([^/]*)?', BusAPIHandler),
    (r'/train/([^/]*)?', TrainAPIHandler),
    ((r'/stops/(%s)/(%s)' % (NUMBER, NUMBER)), NearestStopHandler),
], **settings)

data_manager = None
data_manager_init_thread = None

if __name__ == '__main__':
    parse_command_line()
    data_manager_init_thread = threading.Thread(target=init_data_manager_daemon)
    data_manager_init_thread.setDaemon(True)
    data_manager_init_thread.start()

    schedule_job(60 * 60 * 1, update_data_manager_job)
    schedule_job(60 * 10, update_stop_times_window_job)

    application.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

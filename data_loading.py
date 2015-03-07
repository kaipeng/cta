import pandas as pd


def load_data(data_to_load):
    return pd.DataFrame.from_csv('google_transit/' + data_to_load + '.txt')


def load_stops():
    return load_data('stops')


def load_stop_times():
    return load_data('stop_times')


def load_trips():
    return load_data('trips')


def load_calendar():
    return load_data('calendar')



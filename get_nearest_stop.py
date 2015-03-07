def get_nearest_stop(stops, my_loc):
    my_stops = stops.copy()
    my_stops['my_lat'] = my_loc[0]
    my_stops['my_lon'] = my_loc[1]
    my_stops['dist'] = ((my_stops.my_lat - my_stops.stop_lat) ** 2 + (
        my_stops.my_lon - my_stops.stop_lon) ** 2) ** .5 * 69
    my_stops = my_stops.sort(columns=['dist'])
    return my_stops.head()
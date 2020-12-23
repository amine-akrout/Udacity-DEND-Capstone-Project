# Import Meteostat library and dependencies
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from meteostat import Stations, Daily
import json

#coordinates of Paris
lat = 48.8534
lon = 2.3488

def get_end_date():
	station = Stations()
	station = station.nearby(lat, lon)
	station_end = station.fetch(1).daily_end
	end_day = station_end[0].date()
	end = datetime(end_day.year,end_day.month, end_day.day)
	return end

# Set time period
start = datetime(2016, 1, 1)
end = get_end_date()

stations = Stations()
stations = stations.nearby(lat, lon)
stations = stations.inventory('daily', (start, end))
station = stations.fetch(1)

# Get daily data from 2016 to today
data = Daily(station, start, end)
data = data.fetch()
data.reset_index(level=0, inplace=True)
data.to_json("../../data/weather_data/weather_paris.json", orient="records")
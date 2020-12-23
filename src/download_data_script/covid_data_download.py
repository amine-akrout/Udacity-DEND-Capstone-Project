import csv
import requests


dic = {'confirmed' : 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
'deaths' : 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
'recovered' : 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'
}

for key, value in dic.items():
	req = requests.get(value)
	url_content = req.content
	csv_file = open(f'../data/covid19_data/{key}_data.csv', 'wb')

	csv_file.write(url_content)
	csv_file.close()
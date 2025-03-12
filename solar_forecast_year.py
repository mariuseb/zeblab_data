from metnwp_api import getLatestForecast, forecast_map
import matplotlib.pyplot as plt
import pandas as pd

# test march

start = pd.Timestamp("2024-02-28 23:00")
stop = pd.Timestamp("2024-03-31 01:00")
act_start = pd.Timestamp("2024-03-01 00:00")
act_stop = pd.Timestamp("2024-03-31 00:00")
index = pd.date_range(start, stop, freq="1H")
index =index.tz_localize("Europe/Oslo")

one_hour = pd.DataFrame(
                        index=
                        index,
                        columns=
                        ["Tout", "solGlo", "caf"]
                        )

location = {
    "lat": 63.416,
    "lon": 10.411,
    "tz": "Europe/Oslo"
}

# try first of year:
date = start

#actual = "https://thredds.met.no/thredds/dodsC/metpparchive/2024/01/15/met_analysis_1_0km_nordic_20240115T00Z.nc"
for ndx in index:    
    year = str(ndx.year)
    month = ("0" if ndx.month < 10 else "") + str(ndx.month)
    date = ("0" if ndx.day < 10 else "") + str(ndx.day)
    hour = ("0" if ndx.hour < 10 else "") + str(ndx.hour)
    WEBfo = f"""https://thredds.met.no/thredds/dodsC/metpparchive/{year}/{month}/{date}/met_analysis_1_0km_nordic_{year}{month}{date}T{hour}Z.nc"""
    forecast, time = getLatestForecast(WEBfo, location)
    one_hour.loc[forecast.index[0], :] = forecast.loc[forecast.index[0]]
    
one_hour.to_csv("forecasts_march_2024.csv", index=True)
print(forecast)


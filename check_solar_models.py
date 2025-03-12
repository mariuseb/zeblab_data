# Script by Kristian Skeie 02-2023

from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
from matplotlib import pyplot as plt
import pandas as pd
from datetime import datetime, timezone
from pvlib.location import Location
import strict_rfc3339
import numpy as np
from lib.prepare_df_and_calculate_heating import split_solar, calculate_solargains, calculate_poa
from lib.get_forecast import get_forecast
import datetime as dt
from zoneinfo import ZoneInfo
from metno_locationforecast import Forecast, Place


MY_TOKEN = "dsu8fZNL4p_n3kbucBgIifNSkF8BTptODBfhctSfNZAxbJ5DTx6tQXFZHVfAwuSjAABfSfZnxO99Mv9pg-oqTg=="
client = InfluxDBClient(url="http://eagle.sintef.no:8086",
                        token=MY_TOKEN,
                        org="SINTEF",
                        timeout=1000000)

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

# TOODO: Some queries are fn:mean, others are fn:last. The two should be aligned 
def clip_series_and_remove_outliers(returned_series, clip_to_zero=True):
    returned_series = returned_series[~((returned_series-returned_series.mean()).abs() > returned_series.std())]
    if clip_to_zero == True:
        returned_series.clip(lower=0, inplace=True)
    else:
        returned_series[returned_series<0] = None
    return(returned_series)

def get_observations(
                     res=None,
                     dur=None,
                     start=None,
                     stop=None 
                     ):
    
    if dur is None:
        assert start is not None and stop is not None
        # time range:
        index = pd.date_range(start=start, end=stop, freq=res+"in") # from "{x}m" to "{x}min"
        
        start = strict_rfc3339.timestamp_to_rfc3339_utcoffset(int(start.value/1e9))
        stop = strict_rfc3339.timestamp_to_rfc3339_utcoffset(int(stop.value/1e9))
        range_str = f'|> range(start: {start}, stop: {stop})'
    else:
        range_str = f'|> range(start: -{dur})'
        
    empty_series = pd.Series(index=index, data=np.nan)
        
    """
    lux_setpoint_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
    range_str + \
    '|> filter(fn: (r) => r["Units"] == "luxes")'
    '|> filter(fn: (r) => r["Component"] == "RB601" or r["Component"] == "RB602" or r["Component"] == "RB603" or r["Component"] == "RB605" or r["Component"] == "RB604")'
    '|> filter(fn: (r) => r["Floor"] == "2" or r["Floor"] == "1" or r["Floor"] == "3")'
    '|> filter(fn: (r) => r["Point"] == "Setpoint Lux value")'
    '|> filter(fn: (r) => r["Room"] == "2.19")'
    '|> filter(fn: (r) => r["_field"] == "338.569.Flr02.RSegm219.Lgt.BrgtCtl.LgtSpBrgt" or r["_field"] == "338.569.Flr02.RSegm219.Lgt.BrgtCtl.LgtBrgtEff")'
    f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
    #'|> yield(name: "mean")'
    '|> group(columns: ["_time"])' # mode:"by"
    '|> mean(column: "_value")', data_frame_index='_time')._value.rename("lux_setpoint_219")

    lux_value_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
    range_str + \
    '|> filter(fn: (r) => r["Units"] == "luxes")'
    '|> filter(fn: (r) => r["Component"] == "RB601" or r["Component"] == "RB602" or r["Component"] == "RB603" or r["Component"] == "RB605" or r["Component"] == "RB604")'
    '|> filter(fn: (r) => r["Floor"] == "2" or r["Floor"] == "1" or r["Floor"] == "3")'
    '|> filter(fn: (r) => r["Point"] == "Lux value")'
    '|> filter(fn: (r) => r["Room"] == "2.19")'
    '|> filter(fn: (r) => r["_field"] == "338.569.Flr02.RSegm219.Lgt.BrgtCtl.LgtSpBrgt" or r["_field"] == "338.569.Flr02.RSegm219.Lgt.BrgtCtl.LgtBrgtEff")'
    f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
    #'|> yield(name: "mean")'
    '|> group(columns: ["_time"])' # mode:"by"
    '|> mean(column: "_value")', data_frame_index='_time')._value.rename("lux_value_219")
    """  
     
    TAM = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
    '|> filter(fn: (r) => r["_measurement"] == "570.001")'
    '|> filter(fn: (r) => r["Position"] == "North")'
    '|> filter(fn: (r) => r["_field"] == "Temperature")'
    f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
    #'|> yield(name: "mean")')
    '|> group(columns: ["_time"])' # mode:"by"
    '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_amb").interpolate('time')

    POA = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
    '|> filter(fn: (r) => r["_measurement"] == "570.001")'
    '|> filter(fn: (r) => r["Position"] == "South")'
    '|> filter(fn: (r) => r["_field"] == "Solar_radiation")'
    f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
    #'|> yield(name: "mean")')
    '|> group(columns: ["_time"])' # mode:"by"
    '|> mean(column: "_value")', data_frame_index='_time')._value.rename("I_ver")

    GHI = query_api.query_data_frame('from(bucket: "zeb")'  + \
        range_str + \
    '|> filter(fn: (r) => r["_measurement"] == "570.001")'
    '|> filter(fn: (r) => r["Position"] == "Roof")'
    '|> filter(fn: (r) => r["_field"] == "Solar_radiation")'
    f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
    #'|> yield(name: "mean")')
    '|> group(columns: ["_time"])' # mode:"by"
    '|> mean(column: "_value")', data_frame_index='_time')._value.rename("I_hor")

    # TODO: Cleanup!
    df = pd.DataFrame(TAM)
    df['T_sky'] = TAM-18*0.4
    df['I_ver'] = POA
    df['I_hor'] = GHI
    #df['lux_set_219'] = lux_setpoint_219
    #df['lux_val_219'] = lux_value_219
    
    return df


dur = '400d'
res = '60m'

"""
Enable getting data by start, stop also
"""
location = Location(latitude=63.4144, longitude=10.4092)
start = pd.Timestamp("2024-02-28 22:00")
stop = pd.Timestamp("2024-03-30 23:00")
data = get_observations(res=res, start=start, stop=stop)
data.index = pd.to_datetime(data.index).tz_convert("Europe/Oslo")

forecast = pd.read_csv("forecasts_march_2024.csv", index_col=0)
forecast.index = pd.to_datetime(forecast.index)
forecast = forecast.loc[data.index]
forecast["Tout"] -= 273.15
forecast["solGlo"] /= 3600

data["I_hor_forecast"] = forecast["solGlo"]
data["T_amb_forecast"] = forecast["Tout"]
data["caf_forecast"] = forecast["caf"]

ghi = data["I_hor_forecast"].resample(rule="1min").ffill()
# split solar:
ghi, bni, bhi, dhi = split_solar(ghi, location)
bni = bni.fillna(0)
data["I_ver_forecast"] = bni

fig, axes = plt.subplots(2,1, sharex=True)
ax = axes[0]
data[["I_hor", "I_hor_forecast"]].plot(drawstyle="steps-post", ax=ax)
# use pvlib/DIRINT to get bni
ax = axes[1]
data[["I_ver", "I_ver_forecast"]].plot(drawstyle="steps-post", ax=ax)
plt.show()

# check
#data["I_hor"] = data["I_hor_forecast"]
I_ver_calc = calculate_poa(data, 180)
data["I_ver_calc"] = I_ver_calc.fillna(0)

fig, axes = plt.subplots(3,1, sharex=True)
ax = axes[0]
data[["I_hor", "I_hor_forecast"]].plot(drawstyle="steps-post", ax=ax)
# use pvlib/DIRINT to get bni
ax = axes[1]
data[["I_ver", "I_ver_calc"]].plot(drawstyle="steps-post", ax=ax)
data["I_ver_res"] = data["I_ver"] - data["I_ver_calc"]
data["I_ver_res"].plot(drawstyle="steps-post", ax=ax)
ax = axes[2]
data[["caf_forecast"]].plot(drawstyle="steps-post", ax=ax)
plt.show()

print(forecast)



#data.to_csv("solar_2024_1min.csv", index=True)

# check historical against measured

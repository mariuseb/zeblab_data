# Script by Kristian Skeie 02-2023

from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
from matplotlib import pyplot as plt
import pandas as pd
from datetime import datetime, timezone
import strict_rfc3339
import numpy as np


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
    
    fields = ["338.569.Flr01.RSegm121.SenDev.TR", "338.569.Flr03.RSegm321.SenDev.TR", "338.569.Flr03.RSegm320.SenDev.TR"]
    temps = []
    for field in fields:
        temp = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
         '|> filter(fn: (r) => r["Room"] == "3.20" or r["Room"] == "3.21" or r["Room"] == "1.21")'
         '|> filter(fn: (r) => r["Component"] == "RT601")'
         '|> filter(fn: (r) => r["Floor"] == "3" or r["Floor"] == "1")'
         '|> filter(fn: (r) => r["Point"] == "Room temperature")'
         '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
         '|> filter(fn: (r) => r["_measurement"] == "569.")'
         f'|> filter(fn: (r) => r["_field"] == "{field}")'
         f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time') #._value #.rename(field.split(".")[3])
        #, data_frame_index='_time')._value #.rename(field.split(".")[3])
        
        if temp.empty:
            temp = empty_series
        else:
            temp = temp._value
            #temp.index = temp.index.tz_localize(None)
        temp.name = field.split(".")[3]
        temps.append(temp)
        """
        try:
            temp.index = temp.index.tz_localize(None)
            temps.append(temp._value)
        except AttributeError:
            temps.append(empty_series)
        """
            


    
    fields = ["338.569.220.TCtl.PrSp", "338.569.219.TCtl.PrSp"]
    T_sets = []
    try:
        for field in fields:
            T_set = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
            '|> filter(fn: (r) => r["Room"] == "2.19" or r["Room"] == "2.20")'
            '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
            '|> filter(fn: (r) => r["Point"] == "Act setp heating")'
            f'|> filter(fn: (r) => r["_field"] == "{field}")'
            f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
            #'|> yield(name: "mean")')
            '|> group(columns: ["_time"])'
            '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_set_" + field.split(".")[2])
            #PV_TOT.fillna(method='ffill', inplace=True)
            #PV_TOT.fillna(method='bfill', inplace=True)
            T_sets.append(T_set)
    except AttributeError:
        pass
        
    T_sets = pd.DataFrame(T_sets).T
    
    """
    val_poses = []
    fields = ["338.569.220.TCtl.Cmd", "338.569.219.TCtl.Cmd"]
    for field in fields:
        val_pos = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.19" or r["Room"] == "2.20")'
        '|> filter(fn: (r) => r["Point"] == "Radiator valve position")'
        f'|> filter(fn: (r) => r["_field"] == "{field}")'
        '|> filter(fn: (r) => r["Component"] == "SB501")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")')
        '|> group(columns: ["_time"])'
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("val_" + field.split(".")[2])
        #PV_TOT.fillna(method='ffill', inplace=True)
        #PV_TOT.fillna(method='bfill', inplace=True)
        val_poses.append(val_pos)
    val_pos = pd.DataFrame(val_poses).T
    """ 
    
    #fields = ["338.569.219.TCtl.TR3", "338.569.219.TR1", "338.569.219.TR2", "338.569.219.TR4"]
    fields = ["338.569.219.TCtl.TR3"]
    temps_219 = []
    for field in fields:
        temp = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.19")'
        '|> filter(fn: (r) => r["Point"] == "Room temperature")'
        '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
        f'|> filter(fn: (r) => r["_field"] == "{field}")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_219_" + field.split(".")[-1])
        temps_219.append(temp)
    temps_219 = pd.DataFrame(temps_219).T
    
    #fields = ["338.569.220.TCtl.TR3", "338.569.220.TR1", "338.569.220.TR2", "338.569.220.TR4"]
    fields = ["338.569.220.TCtl.TR3"]
    temps_220 = []
    for field in fields:
        temp = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.20")'
        '|> filter(fn: (r) => r["Point"] == "Room temperature")'
        '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
        f'|> filter(fn: (r) => r["_field"] == "{field}")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_220_" + field.split(".")[-1])
        temps_220.append(temp)
    temps_220 = pd.DataFrame(temps_220).T

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



    """
    val_pos_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
       '|> filter(fn: (r) => r["Component"] == "SB501")'
       '|> filter(fn: (r) => r["Room"] == "2.19")'
       '|> filter(fn: (r) => r["Floor"] == "2")'
       '|> filter(fn: (r) => r["_field"] == "338.569.219.TCtl.Cmd")'
       '|> filter(fn: (r) => r["_measurement"] == "569.219")'
       '|> filter(fn: (r) => r["Point"] == "Radiator valve position")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("val_pos_219")
    """

    try:
        T_207 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.07")'
        '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
        '|> filter(fn: (r) => r["Floor"] == "2")'
        '|> filter(fn: (r) => r["Point"] == "Room temperature")'
        '|> filter(fn: (r) => r["Component"] == "RT601")'
        '|> filter(fn: (r) => r["_field"] == "338.569.Flr02.RSegm207.SenDev.TR")'
        '|> filter(fn: (r) => r["_measurement"] == "569.")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_207")
    except AttributeError:
        T_207 = empty_series
    
    try:
        T_211 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.11")'
        '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
        '|> filter(fn: (r) => r["Component"] == "RT601")'
        '|> filter(fn: (r) => r["Floor"] == "2")'
        '|> filter(fn: (r) => r["Point"] == "Room temperature")'
        '|> filter(fn: (r) => r["_field"] == "338.569.Flr02.R211.RHvacCoo.RTemp")'
        '|> filter(fn: (r) => r["_measurement"] == "569.")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_211")
    except AttributeError:
        T_211 = empty_series
        
    try:
        T_213 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.13")'
        '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
        '|> filter(fn: (r) => r["Component"] == "RT601")'
        '|> filter(fn: (r) => r["Floor"] == "2")'
        '|> filter(fn: (r) => r["Point"] == "Room temperature")'
        '|> filter(fn: (r) => r["_field"] == "338.569.Flr02.RSegm213.SenDev.TR")'
        '|> filter(fn: (r) => r["_measurement"] == "569.")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_213")
    except AttributeError:
        T_213 = empty_series
        
    try:    
        T_217 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.17")'
        '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
        '|> filter(fn: (r) => r["Component"] == "RT601")'
        '|> filter(fn: (r) => r["Floor"] == "2")'
        '|> filter(fn: (r) => r["Point"] == "Room temperature")'
        '|> filter(fn: (r) => r["_field"] == "338.569.Flr02.RSegm217.SenDev.TR")'
        '|> filter(fn: (r) => r["_measurement"] == "569.")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_217")
    except AttributeError:
        T_217 = empty_series
    
    
    try:
        """
        P_rad_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.19")'
        '|> filter(fn: (r) => r["Floor"] == "2")'
        '|> filter(fn: (r) => r["Units"] == "kilowatts")'
        '|> filter(fn: (r) => r["Component"] == "OE017")'
        '|> filter(fn: (r) => r["Point"] == "Active effect energy heat")'
        '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.Pwr")'
        '|> filter(fn: (r) => r["_measurement"] == "569.219")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("P_rad_219")
        """
        P_rad_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.19")'
        '|> filter(fn: (r) => r["Floor"] == "2")'
        '|> filter(fn: (r) => r["Units"] == "kilowatts")'
        '|> filter(fn: (r) => r["Component"] == "OE017")'
        '|> filter(fn: (r) => r["Point"] == "Active effect energy heat")'
        '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.Pwr")'
        #'|> filter(fn: (r) => r["_field"] == "=320.003-OE017_Active effect")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("P_rad_219")
        P_rad_220 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Room"] == "2.20")'
        '|> filter(fn: (r) => r["Point"] == "Active effect energy heat")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("P_rad_220")
    except AttributeError:
        P_rad_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
        '|> filter(fn: (r) => r["Component"] == "OE016")'
        '|> filter(fn: (r) => r["Units"] == "kilowatts")'
        '|> filter(fn: (r) => r["Point"] == "Active effect energy")'
        '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.Pwr")'
        '|> filter(fn: (r) => r["_measurement"] == "320.003")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("P_rad_219")
        
    try:
        CO2_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
            range_str + \
          '|> filter(fn: (r) => r["Room"] == "2.19")'
          '|> filter(fn: (r) => r["Units"] == "partsPerMillion")'
          '|> filter(fn: (r) => r["Component"] == "RY601" or r["Component"] == "RY602")'
          '|> filter(fn: (r) => r["Floor"] == "2")'
          '|> filter(fn: (r) => r["Point"] == "CO2 concentration")'
          '|> filter(fn: (r) => r["_field"] == "338.569.219.CO2R1" or r["_field"] == "338.569.219.CO2R2")'
          '|> filter(fn: (r) => r["_measurement"] == "569.219")'
        f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
        #'|> yield(name: "mean")'
        '|> group(columns: ["_time"])' # mode:"by"
        '|> mean(column: "_value")', data_frame_index='_time')._value.rename("P_rad_219")
    except AttributeError:
        CO2_219 = pd.DataFrame()
    
    P_rad_220 = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
     '|> filter(fn: (r) => r["Room"] == "2.20")'
     '|> filter(fn: (r) => r["Floor"] == "2")'
     '|> filter(fn: (r) => r["Units"] == "kilowatts")'
     '|> filter(fn: (r) => r["Component"] == "OE017")'
     '|> filter(fn: (r) => r["Point"] == "Active effect energy heat")'
     '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH17.Pwr")'
     '|> filter(fn: (r) => r["_measurement"] == "569.220")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("P_rad_220")
    

    V_flow_220 = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE017")'
     '|> filter(fn: (r) => r["Point"] == "Flow l/h")'
     '|> filter(fn: (r) => r["Units"] == "litersPerHour")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("V_flow_220")


    V_flow_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE016")'
     '|> filter(fn: (r) => r["Point"] == "Flow l/h")'
     '|> filter(fn: (r) => r["Units"] == "litersPerHour")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("V_flow_219")
    

    T_sup_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE016")'
     '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
     '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.TFl")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_sup_219")
    
    T_ret_219 = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE016")'
     '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
     '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.TRt")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_ret_219")
     
    T_sup_220 = query_api.query_data_frame('from(bucket: "zeb")'  + \
        range_str + \
    '|> filter(fn: (r) => r["_measurement"] == "320.003")'
    '|> filter(fn: (r) => r["Component"] == "OE017")'
    '|> filter(fn: (r) => r["Point"] == "Return temp energy" or r["Point"] == "Flow temp energy")'
    '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
    '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH17.TFl")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_sup_220")
    
    T_ret_220 = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE017")'
     '|> filter(fn: (r) => r["Point"] == "Return temp energy" or r["Point"] == "Flow temp energy")'
     '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
     '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH17.TRt")'
     f'|> aggregateWindow(every: {res}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_ret_220")

    """
    
    PV_TOT = query_api.query_data_frame('from(bucket: "zeb")' + \
        range_str + \
    '|> filter(fn: (r) => r["_measurement"] == "577.001")'
    '|> filter(fn: (r) => r["Point"] == "Energy production Solar panels")'
    '|> aggregateWindow(every: 30m, fn: mean, createEmpty: false)'
    #'|> yield(name: "mean")')
    '|> group(columns: ["_time"])' # mode:"by"
    '|> sum(column: "_value")', data_frame_index='_time')._value.rename("E_pv")    
    """

    #A_fl = 1742

    # TODO: Cleanup!
    df = pd.DataFrame(TAM)
    df['T_sky'] = TAM-18*0.4
    df['I_ver'] = POA
    df['I_hor'] = GHI
    df[temps_219.columns] = temps_219
    df[temps_220.columns] = temps_220
    #df[val_pos.columns] = val_pos
    #bound_temps.index = df.index
    new_index = pd.date_range(df.index[0], df.index[-1], freq="1min")
    df = df.reindex(new_index, fill_value=None)
    df["P_rad_219"] = P_rad_219
    df["P_rad_220"] = P_rad_220
    df["V_flow_219"] = V_flow_219
    df["V_flow_220"] = V_flow_220
    df["T_sup_219"] = T_sup_219
    df["T_sup_220"] = T_sup_220
    df["T_ret_219"] = T_ret_219
    df["T_ret_220"] = T_ret_220
    df["P_rad_219_calc"] = (df["V_flow_219"]/1000)*(df["T_sup_219"] - df["T_ret_219"]) 
    df["P_rad_220_calc"] = (df["V_flow_220"]/1000)*(df["T_sup_220"] - df["T_ret_220"]) 
    # CO2 average:

    #df["P_rad_220"] = P_rad_220

    #df_out = pd.DataFrame({'T_int': T_int,'P_sh': P_sh_real, 'P_ahu': P_ahu_real, 'P_sh_and_ahu': P_ahu_real + P_sh_real, 'E_pv_tot': PV_TOT, 'T_sup_1': T_Sup_1, 'T_sup_2': T_Sup_2, 'T_exh_1': T_exr_1, 'T_exh_2': T_exr_2})

    return df

"""
def get_observations(start='2022-12-01 00:00', stop='2023-02-01 00:00'):
    T_sh = query_api.query_data_frame('from(bucket: "zeb")'
   f'|> range(start: {start}, stop: {stop})'
    '|> filter(fn: (r) => r["Room"] == "2.19")'
    '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
    '|> filter(fn: (r) => r["Component"] == "OE017")'
    '|> filter(fn: (r) => r["Point"] == "Flow temp energy" or r["Point"] == "Return temp energy")'
    '|> aggregateWindow(every: 1H, fn: mean, createEmpty: false)'
    '|> yield(name: "mean")')

    return T_sh
"""

dur = '400d'
res = '1m'

"""
Enable getting data by start, stop also
"""
#start = pd.Timestamp("2022-12-13 00:00")
#stop = pd.Timestamp("2022-12-21 00:00")

#start = pd.Timestamp("2023-11-01 00:00")
#start = pd.Timestamp("2023-11-15 00:00")
#stop = pd.Timestamp("2024-03-01 00:00")
#start = pd.Timestamp("2024-01-13 17:00")
#start = pd.Timestamp("2025-01-12 17:00")
#start = pd.Timestamp("2025-01-30 00:00")
start = pd.Timestamp("2025-02-13 00:00")
stop = pd.Timestamp("2025-02-20 00:00")
#data = get_observations(res=res, start=str(start), stop=str(stop))
data_feb = get_observations(res=res, start=start, stop=stop)
data_feb = data_feb.resample(rule="15min").mean()

start = pd.Timestamp("2025-03-07 00:00")
stop = pd.Timestamp("2025-03-12 00:00")
#data = get_observations(res=res, start=str(start), stop=str(stop))
data_mar = get_observations(res=res, start=start, stop=stop)
data_mar = data_mar.resample(rule="15min").mean()

#data = data.resample(rule="15min").mean()
#data.index = data.index.tz_convert("Europe/Oslo")
#data.to_csv("ZEBLab_nov23_feb24_%s.csv" % (res, ))5
#data.to_csv("ZEBLab_feb25_%s_MPC_219_1min.csv" % (res, ))
#data.to_csv("ZEBLab_dec24_%s.csv" % (res, ))
#ax = data[["T_219_TR1","T_219_TR2","T_219_TR3","T_219_TR4"]].mean(axis=1).plot()
#ax = data["P_rad_220"].plot(color="r")
#ax1 = ax.twinx()
#data[["P_rad_219"]].plot(ax=ax1, color="y")
#plt.show()
#print(data)

print(data_feb)

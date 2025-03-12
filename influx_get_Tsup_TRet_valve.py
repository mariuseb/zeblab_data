# Script by Kristian Skeie 02-2023

from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
from matplotlib import pyplot as plt
import pandas as pd
from datetime import datetime, timezone

MY_TOKEN = "dsu8fZNL4p_n3kbucBgIifNSkF8BTptODBfhctSfNZAxbJ5DTx6tQXFZHVfAwuSjAABfSfZnxO99Mv9pg-oqTg=="
client = InfluxDBClient(url="http://eagle.sintef.no:8086",
                        token=MY_TOKEN,
                        org="SINTEF",
                        timeout=20000)

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

# TOODO: Some queries are fn:mean, others are fn:last. The two should be aligned 

def get_observations(DURATION, RESOLUTION):
    
    """
    T_sup_219 = query_api.query_data_frame('from(bucket: "zeb")'
    f'|> range(start: -{DURATION})'
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE016")'
     '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
     '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.TFl")'
     f'|> aggregateWindow(every: {RESOLUTION}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_sup_219")
       
    T_ret_219 = query_api.query_data_frame('from(bucket: "zeb")'
    f'|> range(start: -{DURATION})'
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE016")'
     '|> filter(fn: (r) => r["Units"] == "degreesCelsius")'
     '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.TRt")'
     f'|> aggregateWindow(every: {RESOLUTION}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_ret_219")

    
    V_flow_219 = query_api.query_data_frame('from(bucket: "zeb")'
    f'|> range(start: -{DURATION})'
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     '|> filter(fn: (r) => r["Component"] == "OE016")'
     '|> filter(fn: (r) => r["Point"] == "Flow l/h")'
     '|> filter(fn: (r) => r["Units"] == "litersPerHour")'
     f'|> aggregateWindow(every: {RESOLUTION}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("V_flow_219")
    
    val_pos_219 = query_api.query_data_frame('from(bucket: "zeb")'
    f'|> range(start: -{DURATION})'
       '|> filter(fn: (r) => r["Component"] == "SB501")'
       '|> filter(fn: (r) => r["Room"] == "2.19")'
       '|> filter(fn: (r) => r["Floor"] == "2")'
       '|> filter(fn: (r) => r["_field"] == "338.569.219.TCtl.Cmd")'
       '|> filter(fn: (r) => r["_measurement"] == "569.219")'
       '|> filter(fn: (r) => r["Point"] == "Radiator valve position")'
     f'|> aggregateWindow(every: {RESOLUTION}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("val_pos_219")
    """
    
    P_rad_219 = query_api.query_data_frame('from(bucket: "zeb")'
    f'|> range(start: -{DURATION})'
     '|> filter(fn: (r) => r["Component"] == "OE016")'
     '|> filter(fn: (r) => r["Units"] == "kilowatts")'
     '|> filter(fn: (r) => r["Point"] == "Active effect energy")'
     '|> filter(fn: (r) => r["_field"] == "338.320.003.MtrH16.Pwr")'
     '|> filter(fn: (r) => r["_measurement"] == "320.003")'
     f'|> aggregateWindow(every: {RESOLUTION}, fn: mean, createEmpty: false)'
     #'|> yield(name: "mean")'
     '|> group(columns: ["_time"])' # mode:"by"
     '|> mean(column: "_value")', data_frame_index='_time')._value.rename("P_rad_219")


    TAM = query_api.query_data_frame('from(bucket: "zeb")'
    f'|> range(start: -{DURATION})'
    '|> filter(fn: (r) => r["_measurement"] == "570.001")'
    '|> filter(fn: (r) => r["Position"] == "North")'
    '|> filter(fn: (r) => r["_field"] == "Temperature")'
    f'|> aggregateWindow(every: {RESOLUTION}, fn: mean, createEmpty: false)'
    #'|> yield(name: "mean")')
    '|> group(columns: ["_time"])' # mode:"by"
    '|> mean(column: "_value")', data_frame_index='_time')._value.rename("T_amb").interpolate('time')
    
    # TODO: Cleanup!
    df = pd.DataFrame(TAM)
    #df['Tsup'] = T_sup_219
    #df['Tret'] = T_ret_219
    #df['V_flow_219'] = V_flow_219 
    #df['val_pos_219'] = val_pos_219
    df['P_rad_219'] = P_rad_219

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

dur = '365d'
res = '1m'

data = get_observations(dur, res)
data.to_csv("ZEBLab_Prad_year_1m.csv", index=True)
#data = pd.read_csv("ZEBLab_Tsup_valve_year_1m.csv")
data = data[1:-1]
#valve_data = pd.read_csv("ZEBLab_valve_year_1m.csv")
"""
Try to plot.
"""
data.Tsup[data["Tsup"] > 43] = 43
data.Tret[data["Tret"] > 43] = 43
data.V_flow_219[data["V_flow_219"] > 0.005] = 0.005
# bfill:
data = data.interpolate()
data.index = pd.to_datetime(data._time)
data.drop(columns=["_time"], inplace=True)
"""
Resample temperatures to one hour
Ts = data[["Tsup", "Tret", "T_amb"]].resample(rule="1H").asfreq()
Ts = Ts.resample(rule="1min").bfill()
data[Ts.columns] = Ts
"""
data = data.resample(rule="15min").mean()

Ta = data["T_amb"].sort_values(ascending=True)
Tsup = data["Tsup"][Ta.index]
#Tsup = data["Tret"][Ta.index]

Ta.index = range(len(Ta))
Tsup.index = range(len(Tsup))
ax = Ta.plot(color="k", linestyle="dashed", drawstyle="steps-post")
ax1 = ax.twinx()
Tsup.plot(ax=ax1, color="r", linewidth=0.5, drawstyle="steps-post")
ax.legend(["Temperature"], loc="upper left")
ax1.legend(["Temperature"], loc="upper right")
plt.show()

ax = data.T_amb.plot(drawstyle="steps-post")
data.Tsup.plot(ax=ax, color="r", linewidth=0.5, linestyle="dashed", drawstyle="steps-post")
data.Tret.plot(ax=ax, color="k", linewidth=0.5, linestyle="dashed", drawstyle="steps-post")
ax1 = ax.twinx()
#data["val_pos_219"].plot(drawstyle="steps-post", ax=ax1, color="b", linewidth=0.5)
data["V_flow_219"].plot(drawstyle="steps-post", ax=ax1, color="b", linewidth=0.5)
plt.show()

"""
Extract data for V_flow > 0.
"""
y_data = data[data.V_flow_219 > 0]
Ta = y_data["T_amb"].sort_values(ascending=True)
Tsup = y_data["Tsup"][Ta.index]

ax = Ta.plot(drawstyle="steps-post")
ax1 = ax.twinx()
(-Tsup).plot(ax=ax1, color="r", linewidth=0.5, linestyle="dashed", drawstyle="steps-post")
plt.show()


Ta.index = range(len(Ta))
Tsup.index = range(len(Ta))
ax = Ta.plot(color="k", linestyle="dashed", drawstyle="steps-post")
ax1 = ax.twinx()
Tsup.plot(ax=ax1, color="r", linewidth=0.5, drawstyle="steps-post")
ax.legend(["Outdoor temperature"], loc="upper left")
ax1.legend(["Supply temperature"], loc="upper right")
plt.show()

print(data)

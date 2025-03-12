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
    

    # TODO: Cleanup!
    df = pd.DataFrame(val_pos_219)
    df['V_flow_219'] = V_flow_219 

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

#data = get_observations(dur, res)
#data.to_csv("ZEBLab_valve_year_1m.csv")
data = pd.read_csv("ZEBLab_valve_year_1m.csv")
"""
Try to plot.
"""
data.V_flow_219[data["V_flow_219"] > 0.005] = 0.005
# bfill:
data = data.interpolate()
data.index = pd.to_datetime(data._time)
data.drop(columns=["_time"], inplace=True)
data = data.resample(rule="30min").mean()


val_219 = data["val_pos_219"].sort_values(ascending=True)
flow_219 = data["V_flow_219"][val_219.index]

val_219.index = range(len(val_219))
flow_219.index = range(len(val_219))
ax = flow_219.plot(color="k", linestyle="dashed", drawstyle="steps-post")
ax1 = ax.twinx()
val_219.plot(ax=ax1, color="r", linewidth=3, drawstyle="steps-post")
ax.legend(["flow [kg/s]"], loc="upper left")
ax1.legend(["valve [%]"], loc="upper right")
plt.show()



ax = data.val_pos_219.plot(drawstyle="steps-post")
ax1 = ax.twinx()
data.V_flow_219.plot(ax=ax1, color="r", linestyle="dashed", drawstyle="steps-post")
plt.show()

print(data)

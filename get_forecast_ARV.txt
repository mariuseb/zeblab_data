import pandas as pd
import requests
import datetime
import json
from zoneinfo import ZoneInfo

"""
Forecasts of generated PV are generated each hour, 
and look 48 hours into the future. Unit is presumably
kW.

"""

TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI0ZGRmMGZkOC03MWVlLTQ3ZjUtYTc3Zi02ZDZlZTQyN2I2MDIiLCJpYXQiOjE3MzczNjAxMjIsImV4cCI6MTc0MDAwNTk5OX0.silZcitbk2Ue1AH5lcWXlJctH_TKmcF4oGhRV1lpSxlnAmPU8soOoVPNej1IiFsJC9QnRlcgJg0cuYNZ3BvLmGfLNra04Tss8rQIDyQgNgC_rnnqocgzZym98NAGR-IRgkDyTc6CpR_raMeFNFl-GPWDdW8yhIVErU8ZQEHEM_qWBtC7cmNmml9U_Upwrj5_nIqVPzqeE4Aor2-djbi5K9J7DT3-QXburfBXfcuazg8mQW3hpfDbT7epOQ870Fe38DaRhdHfDMBUZPmEGFEWN0xjKK7SgxVUWbBZaUekKksHyVo4CLoyyQU44AEBCYt7a5O7hlWqilgjYvYHDlB53g"
url = "https://api.centerdenmark.com/api/v1/dataset/aee59f73-107a-4242-b7c3-a4baa4ad5269/query"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer %s" % (TOKEN, )
}
TZ = "Europe/Oslo"

######### MODIFY THIS ############:
forecast_from = "2025-01-22 14:00"
# to pd.Timestamp in UTC
forecast_from_utc = pd.Timestamp(
                                 forecast_from,
                                 tz=TZ
                                 ).tz_convert("UTC")

request_body = {
  "limit": 100,
  "page": 0,
  "filters":  [   
    {
      "type": "STRING",
      "column": "version",
      "operator": "EQUAL",
      "value": 
        datetime.datetime.strftime(
                forecast_from_utc.to_pydatetime(
                  ZoneInfo(TZ)
                  ),
                '%Y-%m-%dT%H:%M:%S.000Z'
        )
    }
  ],
  "listFilters": [],
  "sortList": []
}

response = requests.post(
                         url,
                         headers=headers,
                         data=json.dumps(request_body)
                         )
test = pd.DataFrame.from_dict(
                    response.json()["data"][0], 
                    orient="index"
                    )
forecast = pd.DataFrame(
                      columns=test.index
                       )

if response.ok:
    print(response.json())
else:
    print(str(response.status_code) + ": " + requests.status_codes._codes[response.status_code][0])

for entry in response.json()["data"]:
    # time instant as index:
    ndx = pd.Timestamp(
                       entry["timeInstant"], 
                       tz=TZ
                       )
    forecast.loc[ndx] = pd.DataFrame.from_dict(
                                               entry, 
                                               orient="index"
                                               ).values.flatten()

forecast = forecast.sort_index()
# skip all except value:
forecast = forecast["value"].astype(float)
# to csv:
forecast.to_csv(
                "forecast_%s.csv" %
                (forecast_from.replace(" ", "_"), )
               )

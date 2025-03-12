# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 07:42:07 2022

@author: hwaln

Module to recieve weather forecast from Met Norway Thredds Service
https://github.com/metno/NWPdocs/wiki/MET-Nordic-dataset
"""

import logging
import netCDF4
import pyproj
import numpy as np
import pandas as pd
import time

logger = logging.getLogger(__name__)

# Create a format to change the data with a for loop
WEB_latest = "https://thredds.met.no/thredds/dodsC/metpplatest/met_forecast_1_0km_nordic_latest.nc"
WEBfo = "https://thredds.met.no/thredds/dodsC/metpparchive/{yyyy}/{mm}/{dd}/met_forecast_1_0km_nordic_{yyyy}{mm}{dd}T{hh}Z.nc"

forecast_map = {
    "Tout": "air_temperature_2m",
    "Pair": "air_pressure_at_sea_level",
    "RH": "relative_humidity_2m",
    "caf": "cloud_area_fraction",
    "WindSpd": "wind_speed_10m",
    "solGlo_long": "integral_of_surface_downwelling_longwave_flux_in_air_wrt_time",
    "solGlo": "integral_of_surface_downwelling_shortwave_flux_in_air_wrt_time",
    "Percip": "precipitation_amount",
    "WindDir": "wind_direction_10m",
}


def getLatestForecast(endpoint, location, history=None, forecasts=["Tout", "solGlo", "caf"]):

    try:
        data = netCDF4.Dataset(endpoint, "r")
    except Exception as err:
        logger.error(f"Not able to connect to database: {err}")
        return None, None

    if history is not None:
        if data.history == history:
            # no update available
            return None, data.history

    try:
        proj4 = (
            "+proj=lcc +lat_0=63 +lon_0=15 +lat_1=63 +lat_2=63 +no_defs +R=6.371e+06"
        )
        proj = pyproj.Proj(proj4)

        # Compute projected coordinates of lat/lon point
        X, Y = proj(location["lon"], location["lat"])

        # Find nearest neighbour
        x = data.variables["x"][:]
        y = data.variables["y"][:]
        Ix = np.argmin(np.abs(x - X))
        Iy = np.argmin(np.abs(y - Y))
        times = pd.DatetimeIndex(
            data.variables["time"][:].data.flatten().astype("datetime64[s]"), tz="UTC"
        )
        variables = {}
        for var in forecasts:
            variables[var] = data.variables[forecast_map[var]][:, Iy, Ix].data.flatten()
        df = pd.DataFrame(index=times, data=variables)
        #if "solGlo" in df.columns:  # derivate solGlo
        #    df.solGlo = df.solGlo.diff().shift(-1) / 3600

        df.index = df.index.tz_convert(location["tz"])
        return df, data.history
    except Exception as err:
        logger.error(f"Not able to extract forecast with error: {err}")
        return None, None

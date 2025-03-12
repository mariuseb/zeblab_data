# Script by Kristian Skeie 02-2023

from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
from matplotlib import pyplot as plt
import pandas as pd
from datetime import datetime, timezone
import strict_rfc3339
import numpy as np


MY_TOKEN = "S8aIE0zDcWEmZMY4YwSYNBdKYmz98aE6BdAV0iF1-qQ6A5KC4VLSmld_5qfsFq-lik_imJZ1kRi599bRUIcFrw=="
client = InfluxDBClient(
                        #username="user_login",
                        #password="user_password",
                        url="http://10.219.43.31:8086",
                        token=MY_TOKEN,
                        org="location_org",
                        verify_ssl=False,
                        timeout=10000
                        )

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

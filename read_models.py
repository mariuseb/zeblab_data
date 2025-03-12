# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 08:36:25 2024

@author: hwaln
"""

import os
import pandas as pd
import numpy as np
import pprint
import matplotlib.pyplot as plt

pp = pprint.PrettyPrinter(indent=4)

idens = [
         file for file in os.listdir()
         if file.startswith("2025")
         and "23:15" in file
         ]
idens.sort()

#df = pd.read_csv("2025-02-14 22:15:00+01:00.csv", index_col=0)

prbs = pd.DataFrame(
    pd.read_csv("2R2C_model.csv", index_col=0)
)
prbs.loc["alpha_int"] = 1
prbs.columns = ["prbs"]

models = pd.DataFrame(
    pd.read_csv("2R2C_start.csv", index_col=0)
)
models.loc["alpha_int"] = 1

dfs = dict()
for i, file in enumerate(idens):
    dfs[i] = pd.read_csv(file, index_col=0)
    models[i+1] = dfs[i].iloc[0][models.index]

print(models)

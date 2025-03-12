import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("model.csv", index_col=0)
_df = pd.read_csv("model_intgains.csv", index_col=0)
ax = _df[["Ti", "y1"]].plot(drawstyle="steps-post")
#df[["Ti", "y1"]].plot(drawstyle="steps-post", color="g", ax=ax)
ax1 = ax.twinx()
#df[["phi_h"]].plot(drawstyle="steps-post", ax=ax1)
plt.show()

print(df)
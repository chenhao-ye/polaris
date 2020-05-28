import json
import pandas as pd
import numpy as np

data = []
f = open("stats.json")
for line in f:
    data.append(json.loads(line.strip()))
df = pd.DataFrame(data=data)
df.to_csv("stats.csv", index=False)
'''
df = df.dropna(how='all',axis=1)
summarized = []
idx = np.where(df.columns.values == 'abort_cnt')[0][0]
for cls, group in df.groupby(list(df.columns[:idx])):
    summarized.append(group.loc[group['throughput'].idxmax()])
summarized = pd.DataFrame(summarized, columns = df.columns).reset_index(drop=True)
summarized.to_csv("stats-summarized.csv", index=False)
'''

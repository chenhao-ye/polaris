import json
import pandas as pd

data = []
f = open("stats.json")
for line in f:
    data.append(json.loads(line.strip()))
df = pd.DataFrame(data=data)
df.to_csv("stats.csv", index=False)


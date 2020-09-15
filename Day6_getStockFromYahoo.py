import requests
import json
import numpy
import pandas

# API位置
start_time = 1596211200
end_time = 1599926400
address = f"https://query1.finance.yahoo.com/v8/finance/chart/2317.TW?period1={start_time}&period2={end_time}&interval=1d&events=history&=hP2rOschxO0"

# 使用requests 來跟遠端 API server 索取資料
response = requests.get(address)

# 序列化資料回報
data = json.loads(response.text)

# 把json格式資料放入pandas中
df = pandas.DataFrame(
    data["chart"]["result"][0]["indicators"]["quote"][0],
    index=pandas.to_datetime(
        numpy.array(data["chart"]["result"][0]["timestamp"]) * 1000 * 1000 * 1000
    ),
)
# 印出前3行：
print(df[:3])
# 印出前5行
print(df.head())

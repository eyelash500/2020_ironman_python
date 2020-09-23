import requests
import pandas
from io import StringIO

# 資料說明：https://data.gov.tw/dataset/11549
# API位置
address = "http://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"

# 取得資料
response = requests.get(address)

# 欄位：證券代號、證券名稱、成交股數、成交金額、開盤價、最高價、最低價、收盤價、漲跌價差、成交筆數
# 會是："00875","國泰網路資安","1,998,885","51,736,728","25.78","25.98","25.77","25.98","+0.46","484"

# 解析
data = response.text
mystr = StringIO(data)
df = pandas.read_csv(mystr, header=None)
# print(df) # 這邊就會印出所有的資料，但是第一行也被認為是資料

# 建立新dataframe
new_headers = df.iloc[0]  # 第一行當作header
df = df[1:]  # 拿掉第一行的資料
df.columns = new_headers  # 設定資料欄位的名稱
print(df)

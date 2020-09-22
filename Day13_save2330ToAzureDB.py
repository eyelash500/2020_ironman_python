import pyodbc
import requests
import json
import numpy
import pandas

server = "ey-finance.database.windows.net"
database = "finance"
username = "我的帳號"
password = "我的密碼"
driver = "{ODBC Driver 17 for SQL Server}"

# API位置
start_time = 1577808000  # 2020/1/1
end_time = 1600531200  # 2020/9/20
stock_code = 2330
stock_market = "TW"
address = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_code}.{stock_market}?period1={start_time}&period2={end_time}&interval=1d&events=history&=hP2rOschxO0"

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
    columns=["open", "high", "low", "close", "volume"],
)

"""匯入資料"""
# 連線到Azure
with pyodbc.connect(
    f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"
) as conn:
    with conn.cursor() as cursor:
        # 把Dataframe 匯入到SQL Server:
        for index, row in df.iterrows():
            cursor.execute(
                """INSERT INTO finance.dbo.DailyPrice 
            (StockID, Symbol, TradeDate, OpenPrice, HighPrice, LowPrice,
            ClosePrice,Volumn)
            values(?,?,?,?,?,?,?,?)""",
                "dd95bd5328b14d489d6f6e649233c774",
                "2330",
                index,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
            )
            conn.commit()
print("finished")

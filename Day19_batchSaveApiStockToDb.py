import requests
import pandas
import pyodbc
import json
import datetime


def convert_date(data_ROC):
    """因為格式為109/1/1，為民國年，需轉換成西元年"""
    date_arr = data_ROC.split("/")
    new_year = int(date_arr[0]) + 1911
    return f"{new_year}-{date_arr[1]}-{date_arr[2]}"


def create_new_header(orignal_headers):
    new_headers = []

    for column in orignal_headers:
        data = str(column)
        if data == "日期":
            new_headers.append("trade_date")
        elif data == "證券名稱":
            new_headers.append("stock_name")
        elif data == "成交股數":
            new_headers.append("volume")
        elif data == "成交金額":
            new_headers.append("total_price")
        elif data == "開盤價":
            new_headers.append("open")
        elif data == "最高價":
            new_headers.append("high")
        elif data == "最低價":
            new_headers.append("low")
        elif data == "收盤價":
            new_headers.append("close")
        elif data == "漲跌價差":
            new_headers.append("spread")
        elif data == "成交筆數":
            new_headers.append("transactions_number")

    return new_headers


def save_data_to_azure_db(symbol, tock_data):
    server = "ey-finance.database.windows.net"
    database = "finance"
    username = "我的帳號"
    password = "我的密碼"
    driver = "{ODBC Driver 17 for SQL Server}"

    with pyodbc.connect(
        f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"
    ) as conn:
        with conn.cursor() as cursor:
            # 把Dataframe 匯入到SQL Server:
            for index, row in tock_data.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO finance.dbo.DailyPrice 
                    (StockID, Symbol, TradeDate, OpenPrice, HighPrice,
                    LowPrice, ClosePrice, Volumn)
                    values(?,?,?,?,?,?,?,?);""",
                        "",
                        symbol,
                        convert_date(row.trade_date),
                        row.open,
                        row.high,
                        row.low,
                        row.close,
                        int(row.volume.replace(",", "")),
                    )
                    conn.commit()
                    # if index % 10 == 0:
                    #     time.sleep(1)
                except Exception as e:
                    print(e)
            return True
    return False


def get_stock_history_data(stock_symbol, his_month):
    # API位置
    # address = "http://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
    stock = stock_symbol
    date = his_month
    address = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock}"

    # 取得資料
    response = requests.get(address)
    # 解析
    # 有幾個部分：stat, date, title, fields, data, notes
    data = response.text  # 這是json格式的資料
    a_json = json.loads(data)  # 轉成dict
    df = pandas.DataFrame.from_dict(a_json["data"])  # 轉成dataframe

    # 修改欄位名稱
    new_headers = create_new_header(a_json["fields"])
    df.columns = new_headers  # 設定資料欄位的名稱
    # print(df)

    _ = save_data_to_azure_db(stock, df)
    print("===finished===")


# 取得從2006-1到現在的所有月份，用此可以取得資料
date_list = (
    pandas.date_range(
        "2006-1-1",
        datetime.datetime.now().strftime("%Y-%m-%d"),
        freq="MS",
        tz="Asia/Taipei",
    )
    .strftime("%Y%m")
    .tolist()
)
print(date_list)

for data_date in date_list:
    print(f"start({datetime.datetime.now().strftime('%m%d-%H%M%S')}): {data_date}")
    get_stock_history_data("2330", data_date)

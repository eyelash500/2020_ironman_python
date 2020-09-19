import pyodbc

server = "ey-finance.database.windows.net"
database = "finance"
username = "我的帳號"
password = "我的密碼"
driver = "{ODBC Driver 17 for SQL Server}"

with pyodbc.connect(
    f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"
) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Stock;")
        row = cursor.fetchone()
        print("This is data in my Azure.....by Eyelash")
        while row:
            print(str(row[0]) + " " + str(row[1]))
            row = cursor.fetchone()

import requests

# API位置
address = "https://query1.finance.yahoo.com/v8/finance/chart/2317.TW?period1=1598889600&period2=1599926400&interval=1d&events=history&=hP2rOschxO0"

# 使用requests 來跟遠端 API server 索取資料
response = requests.get(address)

# 印出取得的結果
print(response.text)
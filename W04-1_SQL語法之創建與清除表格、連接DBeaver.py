import pandas
import jaydebeapi

_sql = [
    "CREATE TABLE IF NOT EXISTS",
    "STOCKINFO(STOCKID CHAR(5), STOCKDATE CHAR(10),",
    "STOCKOPEN NUMBER, STOCKHIGH NUMBER, STOCKLOW NUMBER,",
    "STOCKCLOSE NUMBER, STOCKADJCLOSE NUMBER, STOCKVOL NUMBER);",
]
#如果表格不存在就建立表格，如果存在就不執行下方程式
#STOCKINFO(欄位名稱,資料型態):定義資料表欄位
#CHAR(x):是固定長度的文字，不滿長度會補空白

sql = ' '.join(_sql)
print(sql)

dbConnection =  jaydebeapi.connect(
    "org.h2.Driver",
    "jdbc:h2:D:\Projects\DB\data\H2",
    ["SA", ""],
    "D:/Java/h2/bin/h2-1.4.200.jar")

dbCursor = dbConnection.cursor() #cursor():個人工作區(暫存區)

dbCursor.execute('DROP TABLE STOCKINFO;') #DROP：清除整個資料表，第一次執行(沒有資料表的狀態)不用run此行
dbCursor.execute(sql)
dbCursor.execute('CREATE UNIQUE INDEX ON STOCKINFO(STOCKID, STOCKDATE);')
#UNIQUE INDEX:就是Row ID的意思

dbCursor.close()
dbConnection.close()

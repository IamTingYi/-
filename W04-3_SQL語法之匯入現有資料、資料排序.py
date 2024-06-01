import pandas #用來取得csv
import jaydebeapi #用來取得h2 database

def output_row(row):
    print("%s\t%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f" % row)

_sql = [
  "INSERT INTO",
  "STOCKINFO(STOCKID, STOCKDATE,",
  "STOCKOPEN, STOCKHIGH, STOCKLOW,",
  "STOCKCLOSE, STOCKADJCLOSE, STOCKVOL)",
  "VALUES('%s', '%s', %f, %f, %f, %f, %f, %f);" #定義每個欄位的資料型態
]
sql = ' '.join(_sql)

dbConnection =  jaydebeapi.connect(
    "org.h2.Driver",
    "jdbc:h2:D:/Projects/DB/data/H2",
    ["SA", ""],
    "D:/Java/h2/bin/h2-1.4.200.jar")

dbCursor = dbConnection.cursor()

dbCursor.execute('TRUNCATE TABLE STOCKINFO;')

my_data_path = 'D:/Projects/DB/data'
my_data_file = 'IBM-2017.csv'
my_dataset = pandas.read_csv('%s/%s' % (my_data_path, my_data_file))

for i in range(my_dataset.shape[0]): #shape是取dataframe的dimension(維度) #shape[0]：是取資料筆數
    row = my_dataset.iloc[i]

    insert_sql = sql % ('IBM', row['Date'],
                        row['Open'],
                        row['High'],
                        row['Low'],
                        row['Close'],
                        row['Adj Close'], #欄位名稱要跟csv檔裡面的一模一樣
                        row['Volume'] / 1000.0)

    dbCursor.execute(insert_sql)

#確認資料有成功被insert進資料表
dbCursor.execute('SELECT * FROM STOCKINFO LIMIT 5;') #只看最後5筆資料
resultSet = dbCursor.fetchall()

for row in resultSet:
    output_row(row)

dbCursor.execute('SELECT * FROM STOCKINFO ORDER BY STOCKID, STOCKDATE DESC LIMIT 5;') #由大到小排序
resultSet = dbCursor.fetchall()

for row in resultSet:
    output_row(row)

dbCursor.close()
dbConnection.close()

import random
import jaydebeapi

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
    "jdbc:h2:D:\Projects\DB\data\H2",
    ["SA", ""],
    "D:/Java/h2/bin/h2-1.4.200.jar")

dbCursor = dbConnection.cursor()

dbCursor.execute('TRUNCATE TABLE STOCKINFO;') #TRUNCATE：將表格內的所有資料清除

insert_sql = sql % ('IBM', '2020-10-07',
                    100.0 * random.random(),
                    100.0 * random.random(),
                    100.0 * random.random(),
                    100.0 * random.random(),
                    100.0 * random.random(),
                    1000.0 * random.random())

print(insert_sql)
dbCursor.execute(insert_sql)

dbCursor.execute('SELECT * FROM STOCKINFO;')
resultSet = dbCursor.fetchall() #fetchall()：依次選取每一列/筆資料

for row in resultSet:
    print(row)

dbCursor.close()
dbConnection.close()

import pymysql





connect = pymysql.connect(user='root', password='88888888', db='development')


def initializeCustomsClearanceTable():
    cursor = connect.cursor()

    sql = "CREATE TABLE IF NOT EXISTS CustomsClearance (\
                  MBLNumber VARCHAR(20),\
                  FileName VARCHAR(200) NOT NULL,\
                  ChineseName VARCHAR(100) NOT NULL,\
                  EnglishName VARCHAR(100),\
                  AfterModified INT(1),\
                  HSCode VARCHAR(30),\
                  Quantity DOUBLE,\
                  UnitPrice FLOAT,\
                  TotalPrice FLOAT,\
                  Material VARCHAR(200),\
                  UseFor VARCHAR(200),\
                  TaxRate DOUBLE\
                )"
    try:
        cursor.execute(sql)
        connect.commit()
    except:
        # 发生错误时回滚
        connect.rollback()

    # 关闭不使用的游标对象
    cursor.close()


def saveProductInformation(sql):
    cursor = connect.cursor()
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交修改
        connect.commit()
    except pymysql.Error as e:
        # 发生错误时回滚
        print(e.args[0], e.args[1])
        print("Error SQL Statement: "+sql)
        connect.rollback()


def closeDbConnection():
    connect.close()
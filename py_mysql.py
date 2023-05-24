import pymysql
import pandas as pd

# 打开数据库连接
#
#
# # 创建游标，插入数据，要创建游标,通过游标对象就可对数据库进行增、删、查、改
# # cursor = db.cursor()
# # cursor.execute("select * from name_phone")
# # col = cursor.description
# # data = cursor.fetchall()
# # df = pd.DataFrame(list(data))
# #
# # col = [c[0] for c in col]
# #
# # print(col)
# #
# # # 关闭数据库连接
# # db.close()
# # 'personal_information'
# #
# def connect(host='10.10.11.101', port=3301, user='root', passwd='123456'):
#
#     try:
#         db = pymysql.connect(host=host, port=port, user=user, passwd=passwd)
#     except Exception as e:
#         print("数据库连接失败：%s" % e)
#
#     cursor = db.cursor()
#
#     db.select_db('personal_information')
#     name = 'name_phone'
#
#     cursor.execute("select * from " + name)
#     col = cursor.description
#     data = cursor.fetchall()
#
#     col = [c[0] for c in col]
#     df = pd.DataFrame(list(data), columns = col)
#
#     # pd.read_sql()


# print(col)

import os
import pymysql


class Connect(object):

    def __init__(self, host, user, password, port=3306):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.status = True
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password)
            self.cursor = self.conn.cursor()
        except pymysql.Error as e:
            self.status = False

    def selectDb(self, db):
        try:
            self.conn.select_db(db)
        except pymysql.Error as e:
            print("选择数据库失败：%s" % e)

    def showtableName(self):
        self.cursor.execute('show tables')
        names = self.cursor.fetchall()
        name = [n[0] for n in names]
        return name

    def showDatabases(self):
        self.cursor.execute('show databases')
        databases = self.cursor.fetchall()
        database = [ds[0] for ds in databases]
        return database

    def query(self, tableName):
        self.cursor.execute("SHOW FULL COLUMNS FROM {}".format(tableName))
        data = self.cursor.fetchall()
        col = [c[8] if c[8] != '' else c[0] for c in data]
        self.cursor.execute("select * from " + tableName)
        # col = self.cursor.description
        data = self.cursor.fetchall()

        # col = [c[0] for c in col]
        df = pd.DataFrame(list(data), columns=col)

        return df

    def close(self):
        self.conn.close()
        self.cursor.close()


if __name__ == '__main__':
    connect = Connect(host='10.10.11.101', user='root', password='123456', port=3301)
    connect.selectDb('personal_information')
    print(connect.showtableName())
    print(connect.query('name_phone'))

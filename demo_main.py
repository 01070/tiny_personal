# -- coding: utf-8 --
# @Time : 2023/5/22 14:05
# @Author : Yao Sicheng
from Constants.path import args
import per_info_iden.per_run as per_run
import pandas as pd
from py_mysql import Connect
import sqlalchemy


def main(database, table):
    mysql_addr = '10.10.11.101'
    mysql_username = 'root'
    mysql_password = '123456'
    mysql_port = 3301
    # pd.read_sql()
    connectMysql = Connect(mysql_addr, mysql_username, mysql_password, mysql_port)
    if connectMysql.status:

        print("连接成功！")

        all_databases_from_mysql = connectMysql.showDatabases()
        print(all_databases_from_mysql)
        choose_database = database
        print('您选择的是{}'.format(choose_database))
        connectMysql.selectDb(choose_database)
        all_table_from_mysql = connectMysql.showtableName()
        print(all_table_from_mysql)
        choose_table = table

        choose_dataframe = connectMysql.query(choose_table)
        connectMysql.close()
        per_run.run(choose_dataframe, table_name=choose_table, recall_mode=True)
        print("写入完成")

def select():
    engine = sqlalchemy.create_engine("mysql+pymysql://root:123456@10.10.11.101:3301/personal_information")
    sql = "select * from information"
    data_df = pd.read_sql(sql, engine)



if __name__ == '__main__':

    main(args.database, args.table)
    # select()

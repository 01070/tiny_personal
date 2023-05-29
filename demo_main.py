# -- coding: utf-8 --
# @Time : 2023/5/22 14:05
# @Author : Yao Sicheng
from Constants.path import args
import per_info_iden.per_run as per_run
import pandas as pd
from py_mysql import Connect
import sqlalchemy
from time import time
from tqdm import tqdm


def show_time(func):
    def wrapper(*args, **kwargs):
        star = time()
        func(*args, **kwargs)
        end = time()
        print('spend_time:', end - star)

    return wrapper


@show_time
def main(database, table):
    mysql_addr = '10.10.11.101'
    mysql_username = 'root'
    mysql_password = '123456'
    mysql_port = 3301
    # pd.read_sql()
    connectMysql = Connect(mysql_addr, mysql_username, mysql_password, mysql_port)
    if connectMysql.status:

        print("连接成功！")
        connectMysql.selectDb('identification')
        # identification_all_databases_from_mysql = connectMysql.showDatabases()

        all_databases_from_mysql = connectMysql.showDatabases()
        print(all_databases_from_mysql)
        choose_database = database
        print('您选择的是{}'.format(choose_database))
        connectMysql.selectDb(choose_database)
        all_table_from_mysql = connectMysql.showtableName()
        engine = sqlalchemy.create_engine("mysql+pymysql://root:123456@10.10.11.101:3301/identification")
        for choose_table in tqdm(all_table_from_mysql):

            try:
                choose_dataframe = connectMysql.query(choose_table)
            except:
                print("{}读取失败".format(choose_table))
                continue
            output_dataframe_list_concat = per_run.run(choose_dataframe, recall_mode=True)
            if output_dataframe_list_concat is not None:
                try:
                    output_dataframe_list_concat.to_sql(choose_table, engine, index=None)
                except ValueError:
                    print("{}已经存在请检查".format(choose_table))
                print("写入完成")
        connectMysql.close()


def select(table_name):
    engine = sqlalchemy.create_engine("mysql+pymysql://root:123456@10.10.11.101:3301/personal_information")

    sql = "SELECT * FROM {}".format(table_name)
    data_df = pd.read_sql(sql, engine)
    return data_df


if __name__ == '__main__':
    # main(args.database, args.table)
    # select()
    mysql_addr = '10.10.11.101'
    mysql_username = 'root'
    mysql_password = '123456'
    mysql_port = 3301
    # pd.read_sql()
    connectMysql = Connect(mysql_addr, mysql_username, mysql_password, mysql_port)
    if connectMysql.status:

        print("连接成功！")
        connectMysql.selectDb('identification')
        identification_all_databases_from_mysql = connectMysql.showDatabases()

        all_databases_from_mysql = connectMysql.showDatabases()
        print(all_databases_from_mysql)
        choose_database = 'personal_information'
        print('您选择的是{}'.format(choose_database))
        connectMysql.selectDb(choose_database)
        choose_dataframe = connectMysql.query('jiaxing')
        connectMysql.selectDb(choose_database)
        all_table_from_mysql = connectMysql.showtableName()
        engine = sqlalchemy.create_engine("mysql+pymysql://root:123456@10.10.11.101:3301/identification")



        output_dataframe_list_concat = per_run.run(choose_dataframe, recall_mode=True)
        output_dataframe_list_concat.to_sql('jiaxing2', engine, index=None)

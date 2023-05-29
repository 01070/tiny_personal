import os

import pandas as pd

from . import per_info as pi


def mkdir(path):
    # os.path.exists 函数判断文件夹是否存在
    folder = os.path.exists(path)

    # 判断是否存在文件夹如果不存在则创建为文件夹
    if not folder:
        # os.makedirs 传入一个path路径，生成一个递归的文件夹；如果文件夹存在，就会报错,因此创建文件夹之前，需要使用os.path.exists(path)函数判断文件夹是否存在；
        os.makedirs(path)  # makedirs 创建文件时如果路径不存在会创建这个路径
        print('文件夹创建成功：', path)

    else:
        print('文件夹已经存在：', path)


def info_identification(data, recall_mode=False):
    # 如果是张空表，直接跳过
    if len(data) == 0:
        return None

    # pi.query_all_fields中tempo_column_list用于生成识别记录保存，tempo_column_list列名
    tempo_list, tempo_records_list, tempo_column_list, global_records_list = pi.query_all_fields(data,
                                                                                recall_mode=recall_mode)
    keys = ['phone_records', 'ID_records', 'bank_records', 'car_id_records', 'name_records']
    # 用于保存所有的字段识别到的结果
    output_dataframe_list = []
    for record_index in range(len(tempo_records_list)):
        # 若这个记录是空的，直接跳过
        if tempo_records_list[record_index] is None:
            continue
        field_name = tempo_column_list[record_index]
        # 记录所有出现的记录行
        # 获取原始数据

        global_records = global_records_list[record_index]

        if len(global_records) > 0:

            raw_data = data.iloc[global_records, record_index]

            # 用于整合识别到的数据与原始字段表
            output_dataframe = pd.DataFrame(data=None,
                                            columns=[field_name, field_name+'phone_records', field_name+'ID_records', field_name+'bank_records',
                                                     field_name+'car_id_records',
                                                     field_name+'name_records'])
            output_dataframe[field_name] = raw_data
        else:
            output_dataframe = None
        #    for record_index in [13]:
        tempo_record_df_list = []
        record = tempo_records_list[record_index]
        if record is not None:
            # null_test = True
            for key in keys:
                # nested_record_tuples = []
                for value, index_list in list(record[key].items()):
                    for ls_index in index_list:
                        # TODO debug
                        # nested_record_tuples.append(str((value, ls_index)))
                        output_dataframe.loc[ls_index, field_name+key] = str(value)
            # TODO 写到sql中
            if output_dataframe is not None:
                output_dataframe_list.append(output_dataframe)
    if len(output_dataframe_list) == 0:
        return None
    else:
        return pd.concat(output_dataframe_list, axis=1)

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


def info_identification(data, tableName, path2="Intermediate_data/per_info/识别记录保存", ner_func="lac", recall_mode=False):

    if not os.path.exists(path2):
        mkdir(path2)

    # pi.query_all_fields中tempo_column_list用于生成识别记录保存，tempo_column_list列名
    tempo_list, tempo_records_list, tempo_column_list = pi.query_all_fields(data, ner_func=ner_func,
                                                                            recall_mode=recall_mode)

    # tempo_df_fre = pd.DataFrame({
    #     'filecode': str(tableName),
    #     # 'title': tempo_title,
    #     'items': tempo_list[0],
    #     'ratio': tempo_list[1],
    #     'sample': tempo_list[2],
    #     'test_res': tempo_list[3]
    # })
    keys = ['phone_records', 'ID_records', 'bank_records', 'car_id_records', 'name_records']
    for record_index in range(len(tempo_records_list)):
        #    for record_index in [13]:
        tempo_record_df_list = []
        record = tempo_records_list[record_index]
        if record is not None:
            null_test = True
            for key in keys:
                nested_record_tuples = []
                for value, index_list in list(record[key].items()):
                    for ls_index in index_list:
                        # TODO debug
                        nested_record_tuples.append(str((value, ls_index)))

                if nested_record_tuples:
                    null_test = False
                tempo_record_df = pd.DataFrame(nested_record_tuples, columns=[key])
                tempo_record_df_list.append(tempo_record_df)
            try:
                if null_test == False:
                    item_record_df = pd.concat(tempo_record_df_list, axis=1)
                    file_path2 = os.path.join(path2, str(tableName) + '#_#' + str(record_index) + '#_#' + str(
                        tempo_column_list[record_index]).replace('/', '&') + '.xlsx')
                    item_record_df.to_excel(file_path2)
                    # item_record_df.to_sql()
                else:
                    continue
            except:
                continue

    # file_path = os.path.join(path1, 'proc_' + str(tableName) + '.xlsx')
    # tempo_df_fre.to_excel(file_path)

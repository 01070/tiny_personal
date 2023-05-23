import os
import numpy as np
import pandas as pd
import Constants.tool as tl


def Decode(processed_df):
    """
    第一次对检测结果字典集的解码，拆包出各检测结果的字典集。
    """
    res_list = []
    to_drop_row_ls = []
    for row_index in processed_df.index:
        res, none_test = nanTest(processed_df.loc[row_index]['test_res'], row_index)
        res_list.append(res)
    processed_df = processed_df.drop(to_drop_row_ls, axis=0)
    concat_df = pd.concat(res_list, axis=0)
    res_df = pd.concat([processed_df, concat_df], axis=1)
    return res_df


def nanTest(str_dict_item, row_index):
    """
    空集检测，若为非空集则进行字典集转换DataFrame行，若为空集则输出空行。
    """
    none_test = False
    if type(str_dict_item) == str:
        dict_item = eval(str_dict_item)
        tempo_row_df = decodeValue(dict_item, row_index)
        return tempo_row_df, none_test
    else:
        none_df = nanDf(row_index)
        return none_df, True


def decodeValue(dict_item, row_index):
    """
    字典集转换为DataFrame行。
    """
    return pd.DataFrame([dict_item], index=[row_index])


def nanDf(row_index):
    """
    对第一次解码过程中出现字典集空集情况的处理。
    """
    none_df = pd.DataFrame({
        'de_id_res': None,
        'phones_res': None,
        'ID_res': None,
        'bank_res': None,
        'name_res': None,
        'car_id_res': None
    },
        index=[row_index])
    return none_df


def secondDecode(res_df):
    """
    第二次解码，将各检测结果拆包，拆包出各检测结果的详细情况。
    """
    columns_list = res_df.columns.tolist()
    res_columns = columns_list[-6:]
    tempo_res_df = res_df[res_columns]
    final_df_list = []
    for row_index in tempo_res_df.index:
        if res_df['test_res'].loc[row_index] is np.nan:
            final_df_list.append(nanDf2(row_index))
        else:
            tempo_df = []
            for column in res_columns:
                keys = tempo_res_df[column].loc[row_index].keys()
                values = []
                for value in list(tempo_res_df[column].loc[row_index].values()):
                    if type(value) is tuple:
                        value = str(value)
                    values.append(value)
                new_dict = dict(zip(keys, values))
                tempo_df.append(pd.DataFrame(new_dict, index=[row_index]))
            tempo_concat_df = pd.concat(tempo_df, axis=1)
            final_df_list.append(tempo_concat_df)
    final_df = res_df.merge(pd.concat(final_df_list, axis=0), left_index=True, right_index=True).drop(res_columns,
                                                                                                      axis=1)
    return final_df


def nanDf2(row_index):
    """
    对第二次解码过程中出现字典集空集情况的处理。
    """
    none_df = pd.DataFrame({
        'de_id_ratio': None,
        'de_id_test': None,
        'phones': None,
        'phone_test': None,
        'phone_extract': None,
        'ID': None,
        'ID_test': None,
        'ID_extract': None,
        'bank': None,
        'bank_test': None,
        'bank_extract': None,
        'name': None,
        'name_test': None,
        'name_extracte': None,
        'car_id': None,
        'car_id_test': None,
        'car_id_extract': None
    },
        index=[row_index])
    return none_df


def run():

    path = 'Intermediate_data/per_info/检测预处理结果'
    list_files = tl.getFlist(path)

    # 读取预处理结果并合并
    df_list = []
    for file in list_files:
        file_path = os.path.join(path, file)
        df_list.append(pd.read_excel(file_path))
    processed_df = pd.concat(df_list, axis=0).reset_index().drop(['index', 'Unnamed: 0'], axis=1)

    # 两次解码
    res_df = Decode(processed_df)
    final_df = secondDecode(res_df)

    final_df['de_id_test'] = 0
    final_df['de_id_test'] = final_df[final_df['de_id_ratio'] > 0.1]['de_id_test'].apply(lambda x: 1)

    return final_df

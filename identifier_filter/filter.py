import os
import sys
import warnings
import pandas as pd
from . import gadget as gt
from Constants import constant as ct
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))


def read_data(final_data):

    df_privacy = final_data

    df_checked = df_privacy[ct.columns_to_retain].copy()
    df_privacy[ct.check] = df_privacy[ct.origin]
    df_checked[ct.identifier] = 0
    de_id = df_privacy[df_privacy['de_id_test'] == 1]

    return df_privacy, df_checked, de_id


def direct_identifier(df_privacy, df_checked):
    # 没有进行去标识化的数据记录中筛选出存在各项直接标识符暴露的字段

    name_expose = df_privacy[df_privacy['name_test'] == 1]
    name_index_list = name_expose.index.values.tolist()
    phone_expose = df_privacy[df_privacy['phone_test'] == 1]
    phone_index_list = phone_expose.index.values.tolist()
    id_expose = df_privacy[df_privacy['ID_test'] == 1]
    id_index_list = id_expose.index.values.tolist()
    bank_expose = df_privacy[df_privacy['bank_test'] == 1]
    bank_index_list = bank_expose.index.values.tolist()
    car_id_expose = df_privacy[df_privacy['car_id_test'] == 1]
    car_id_index_list = car_id_expose.index.values.tolist()

    gt.deal(name_expose, 'name', 50, name_index_list, df_privacy, df_checked)
    gt.deal(phone_expose, 'phones', 20, phone_index_list, df_privacy, df_checked)
    gt.deal(id_expose, 'ID', 50, id_index_list, df_privacy, df_checked)
    gt.deal(bank_expose, 'bank', 50, bank_index_list, df_privacy, df_checked)
    gt.deal(car_id_expose, 'car_id', 20, car_id_index_list, df_privacy, df_checked)

    identifier_list = []
    for row_index in range(len(df_checked)):
        test = 0
        if df_checked[ct.identifier].iloc[row_index].sum() >= 1:
            test = 1
        identifier_list.append(test)
    df_checked['exp_identifier'] = identifier_list


def de_identifier(de_id, df_checked):
    # 该部分是检测在进行了去标识化字段的数据记录是否指向个人信息

    items_name = de_id['items'].unique().tolist()
    # TODO 表名 + 字段
    # print("存在潜在去标识化的所有字段:")
    text = "" if len(items_name) == 0 else items_name
    # print(text)
    if text != "":
        index_list = de_id.index.values.tolist()
        de_id_diff = list(set(index_list))
        df_checked['deidentified'] = 0
        df_checked['deidentified'][de_id_diff] = df_checked['deidentified'][de_id_diff].apply(lambda x: 1)
    else:
        de_id_diff = []
        df_checked['deidentified'] = 0
        df_checked['deidentified'][de_id_diff] = df_checked['deidentified'][de_id_diff].apply(lambda x: 1)


def quasi_identifier(df_checked, df_privacy):

    df_to_match = df_checked[(df_checked['exp_identifier'] == 0) & (df_checked['deidentified'] == 0)]
    qid_keywords = ct.default_qid_keywords
    to_drop_keyword_list = ct.default_to_drop_keyword_list
    index_list = []

    for row_index in range(len(df_to_match)):
        item = df_to_match['items'].iloc[row_index]
        for keyword in qid_keywords:
            skip = False
            for to_drop_keyword in to_drop_keyword_list:
                if to_drop_keyword in item:
                    skip = True

            if keyword in item and skip == False:
                index_list.append(df_to_match[df_to_match['items'] == item].index.values.tolist())

    res = []
    for i in index_list:
        for j in i:
            res.append(j)
    res = list(set(res))

    df_to_match['qidentifier'] = 0
    df_to_match['qidentifier'][res] = df_to_match['qidentifier'][res].apply(lambda x: 1)

    # 筛选后需要添加的字段名，内容为完全匹配，而非包含关系
    index_to_add_list = []
    res_add = []
    for i in index_to_add_list:
        for j in i:
            res_add.append(j)
    res_add = list(set(res_add))

    res_all = res + res_add
    df_checked['qidentifier'] = 0
    df_checked['qidentifier'][res_all] = df_checked['qidentifier'][res_all].apply(lambda x: 1)

    df_checked2 = df_checked[ct.to_retain1]
    df_privacy2 = df_privacy[ct.to_retain2]

    df_final = df_privacy2.merge(df_checked2, left_index=True, right_index=True)

    path = 'Intermediate_data/per_info/'
    if not os.path.exists(path):
        os.makedirs(path)
    df_final.to_excel(os.path.join(path, '标识符识别结果.xlsx'))

    return df_final


def run(final_data):

    warnings.filterwarnings("ignore")

    df_privacy, df_checked, de_id = read_data(final_data)
    direct_identifier(df_privacy, df_checked)

    de_identifier(de_id, df_checked)

    df_final = quasi_identifier(df_checked, df_privacy)

    return df_final



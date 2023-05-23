import pandas as pd
import numpy as np
import os

from Constants import tool as tl, constant as ct
import Constants.data_processing as dp
from Constants.path import args
import warnings

warnings.filterwarnings("ignore")

import seaborn as sns
import matplotlib.pyplot as plt


def read_data(df_final, record_path='Intermediate_data/per_info/识别记录保存'):
    df = df_final
    df_groupped = df.groupby(['filecode']).sum().reset_index()
    df_groupped_sort = df_groupped

    filecode_list = df['filecode'].unique().tolist()
    exp_dataset_list = []
    deid_dataset_list = []
    no_exp_dataset_list = []
    for tempo_filecode in filecode_list:
        if df[df['filecode'] == tempo_filecode]['exp_identifier'].sum() >= 1:
            exp_dataset_list.append(df[df['filecode'] == tempo_filecode].index.values.tolist())
        else:
            no_exp_dataset_list.append(df[df['filecode'] == tempo_filecode].index.values.tolist())
            if df[df['filecode'] == tempo_filecode]['deidentified'].sum() >= 1:
                deid_dataset_list.append(df[df['filecode'] == tempo_filecode].index.values.tolist())

    res_exp = []
    for i in exp_dataset_list:
        for j in i:
            res_exp.append(j)
    res_no_exp = []
    for i in no_exp_dataset_list:
        for j in i:
            res_no_exp.append(j)
    res_de_id = []
    for i in deid_dataset_list:
        for j in i:
            res_de_id.append(j)

    df_exp = df.iloc[res_exp]
    df_no_exp = df.iloc[res_no_exp]
    df_de_id = df.iloc[res_de_id]

    record_list = tl.walkFile(record_path)

    record_dict = {}
    for record in record_list:
        #     if len(record.replace('.xlsx', '').split('_')) > 3:
        #         for index, content in enumerate(record.replace('.xlsx', '').split('_')):
        #             if content.isdigit():
        #                 record_dict["".join(record.replace('.xlsx', '').split('_')[0:index]), int(content)] = record
        #                 break
        #     else:
        split_res = record.replace('.xlsx', '').rsplit('#_#', 2)
        record_dict[str(split_res[0]), int(split_res[1])] = record
    record_tuple_list = list(record_dict.keys())

    exp_index_list = df_exp['filecode'].unique().tolist()

    # if not os.path.exists(args.save_path) and not os.path.exists(args.save_path) \
    #         and not os.path.exists(args.save_path):
    #     tl.mkdir(args.save_path)
    #     tl.mkdir(args.save_path2)
    #     tl.mkdir(args.save_path3)

    return df, df_groupped_sort, df_exp, exp_index_list, record_dict, record_tuple_list


def data_split(df_exp, table_name, exp_index_list, record_dict, record_tuple_list, table_data,
               record_path='Intermediate_data/per_info/识别记录保存'):
    """
    根据记录行是否含直接标识符对数据集拆分重组
    :param df:
    :param record_path:
    :param path:
    :return:
    """

    keys_mapping_dict = dict(zip(ct.test_keys, ct.checked_keys))

    df_new_exp_index = []
    df_Ab_index = []
    df_Ab_risk_null = []

    id_records = {}
    direct_result = []
    for index in exp_index_list:
        if index == table_name:
            id_records[index] = {}

            record_index_list = []
            tempo = df_exp[df_exp['filecode'] == index]
            tempo_columns_list = tempo['items'].tolist()

            tempo_read_df = table_data  # 原表数据

            tempo_read_columns_list = [x.replace('\n', '').replace('\r', '').replace('_x000D_', '') for x in
                                       tempo_read_df.columns.tolist()]
            tempo_read_df.columns = tempo_read_columns_list
            tempo_read_df[ct.test_keys] = ''

            title_item_index_set_list = [(index, item_index) for item_index in
                                         range(len(tempo_read_df.columns.tolist()))]
            tempo_series_list = []
            ts_df_list = []
            for title_item_index_set in title_item_index_set_list:
                if title_item_index_set in record_tuple_list:
                    title_index, item_index = title_item_index_set
                    target_column_name = tempo_read_columns_list[item_index]

                    df_new_exp_index.append(title_index)
                    record_file_name = record_dict[title_item_index_set]
                    tempo_target_item = tempo[tempo['items'] == target_column_name]
                    if len(tempo_target_item) > 1:
                        print('存在重名字段')
                    if tempo_target_item['exp_identifier'].iloc[0] == 0:
                        not_exp_test = True
                    else:
                        not_exp_test = False

                    if not_exp_test:
                        continue
                    record_file_path = os.path.join(record_path, record_file_name)
                    tempo_record = pd.read_excel(record_file_path)
                    tempo_series_list = []
                    for test_key in ct.test_keys:
                        record_ls = list(tempo_record[test_key].dropna())
                        checked_key = keys_mapping_dict[test_key]

                        if tempo_target_item[checked_key].iloc[0] == 0:
                            not_num_test = True
                        else:
                            not_num_test = False

                        if not_num_test:
                            continue

                        if record_ls:
                            record_ls = [eval(x) for x in record_ls]
                            value_ls = []
                            index_ls = []
                            for record_set in record_ls:
                                record_value, record_index = record_set
                                value_ls.append(record_value)
                                index_ls.append(record_index)
                                record_index_list.append(record_index)
                            tempo_series = pd.Series(value_ls, index=index_ls, name=test_key)
                            duplicated_index = tempo_series[tempo_series.index.duplicated()].index.unique()
                            for i in duplicated_index:
                                tempo_series.loc[i] = ";".join(set(tempo_series.loc[i]))
                            tempo_series = tempo_series.groupby(tempo_series.index).first()
                            tempo_series = tempo_series + ';'
                            tempo_series_list.append(tempo_series)
                            tempo_read_df[test_key] = tempo_read_df[test_key].fillna('')
                            for se_index in tempo_series.index:
                                tempo_read_df[test_key].loc[se_index] = tempo_read_df[test_key].loc[se_index] + \
                                                                        tempo_series.loc[se_index]

            record_index_list_drop_dup = list(set(record_index_list))
            df_index_list = list(tempo_read_df.index.values)
            rest_index_list = list(set(df_index_list) - set(record_index_list_drop_dup))
            save_file1_path = os.path.join(args.save_path, '直接暴露' + '_' + str(index) + '.xlsx')
            save_df1 = tempo_read_df.loc[record_index_list_drop_dup]

            tempo_exp_identifier_items = tempo[tempo['exp_identifier'] == 1]['items'].tolist()
            tempo_qidentifier_items = tempo[tempo['qidentifier'] == 1]['items'].tolist()
            subsets = tempo_exp_identifier_items
            save_df1_drop_duplicates = save_df1.drop_duplicates(subset=subsets)

            if not save_df1_drop_duplicates.empty:
                df_Ab_index.append(index)
                save_df1_drop_duplicates.to_excel(save_file1_path)
                direct_result.append(save_df1_drop_duplicates)
            else:
                df_Ab_risk_null.append(index)
            save_file2_path = os.path.join(args.save_path2, '无直接暴露' + '_' + str(index) + '.xlsx')
            save_df2 = tempo_read_df.loc[rest_index_list]
            save_df2.to_excel(save_file2_path)

            info_mapping_dict = {}
            for info_type in ct.info_type_ls:
                info_mapping_dict[info_type] = []
            qid_length_map_dict = {}

            for test_key in ct.test_keys:
                id_records[index][test_key] = len(
                    [x.strip() for x in save_df1_drop_duplicates[test_key].astype(str).dropna() if x.strip() != ''])
            for tempo_qidentifier_item in tempo_qidentifier_items + tempo_exp_identifier_items:
                qid_exposed_length = len(
                    [x.strip() for x in save_df1_drop_duplicates[tempo_qidentifier_item].astype(str).dropna() if
                     x.strip() != ''])
                id_records[index][tempo_qidentifier_item] = qid_exposed_length

                for i in range(len(ct.class_keyword_list)):
                    tempo_keyword_list = ct.class_keyword_list[i]
                    for keyword in tempo_keyword_list:
                        if keyword in tempo_qidentifier_item:
                            info_mapping_dict[ct.info_type_ls[i]].append(tempo_qidentifier_item)
                            qid_length_map_dict[tempo_qidentifier_item] = qid_exposed_length

            for info_type in ct.info_type_ls:
                tempo_info_qid_ls = info_mapping_dict[info_type]
                qid_length_ls = []
                exp_length_ls = []

                try:
                    tempo_info_exp_ls = ct.exp_info_mapping_dict[info_type]
                    for tempo_info_exp in tempo_info_exp_ls:
                        exp_length_ls.append(id_records[index][tempo_info_exp])
                    if exp_length_ls:
                        max_exp_length = max(exp_length_ls)
                    else:
                        max_exp_length = 0
                except:
                    max_exp_length = 0

                for tempo_info_qid in tempo_info_qid_ls:
                    qid_length_ls.append(qid_length_map_dict[tempo_info_qid])

                if qid_length_ls:
                    max_qid_length = max(qid_length_ls)
                else:
                    max_qid_length = 0

                id_records[index][info_type] = max(max_exp_length, max_qid_length)

    return id_records, direct_result


def data_split_source(df_exp, exp_index_list, record_dict, record_tuple_list, table_data, record_path='识别记录保存'):
    """
    根据记录行是否含直接标识符对数据集拆分重组
    :param df:
    :param record_path:
    :param path:
    :return:
    """

    keys_mapping_dict = dict(zip(ct.test_keys, ct.checked_keys))

    df_new_exp_index = []
    df_Ab_index = []
    df_Ab_risk_null = []

    id_records = {}
    direct_result = []
    for index in exp_index_list:

        id_records[index] = {}

        record_index_list = []
        tempo = df_exp[df_exp['filecode'] == index]
        tempo_columns_list = tempo['items'].tolist()

        tempo_read_df = table_data  # 原表数据

        tempo_read_columns_list = [x.replace('\n', '').replace('\r', '').replace('_x000D_', '') for x in
                                   tempo_read_df.columns.tolist()]
        tempo_read_df.columns = tempo_read_columns_list
        tempo_read_df[ct.test_keys] = ''

        title_item_index_set_list = [(index, item_index) for item_index in range(len(tempo_read_df.columns.tolist()))]
        tempo_series_list = []
        ts_df_list = []
        for title_item_index_set in title_item_index_set_list:
            if title_item_index_set in record_tuple_list:
                title_index, item_index = title_item_index_set
                target_column_name = tempo_read_columns_list[item_index]

                df_new_exp_index.append(title_index)
                record_file_name = record_dict[title_item_index_set]
                tempo_target_item = tempo[tempo['items'] == target_column_name]
                if len(tempo_target_item) > 1:
                    print('存在重名字段')
                if tempo_target_item['exp_identifier'].iloc[0] == 0:
                    not_exp_test = True
                else:
                    not_exp_test = False

                if not_exp_test:
                    continue
                record_file_path = os.path.join(record_path, record_file_name)
                tempo_record = pd.read_excel(record_file_path)
                tempo_series_list = []
                for test_key in ct.test_keys:
                    record_ls = list(tempo_record[test_key].dropna())
                    checked_key = keys_mapping_dict[test_key]

                    if tempo_target_item[checked_key].iloc[0] == 0:
                        not_num_test = True
                    else:
                        not_num_test = False

                    if not_num_test:
                        continue

                    if record_ls:
                        record_ls = [eval(x) for x in record_ls]
                        value_ls = []
                        index_ls = []
                        for record_set in record_ls:
                            record_value, record_index = record_set
                            value_ls.append(record_value)
                            index_ls.append(record_index)
                            record_index_list.append(record_index)
                        tempo_series = pd.Series(value_ls, index=index_ls, name=test_key)
                        duplicated_index = tempo_series[tempo_series.index.duplicated()].index
                        for i in duplicated_index:
                            tempo_series.loc[i] = str(tempo_series.loc[i].tolist())
                        tempo_series = tempo_series.groupby(tempo_series.index).first()
                        tempo_series = tempo_series + ';'
                        tempo_series_list.append(tempo_series)
                        tempo_read_df[test_key] = tempo_read_df[test_key].fillna('')
                        for se_index in tempo_series.index:
                            tempo_read_df[test_key].loc[se_index] = tempo_read_df[test_key].loc[se_index] + \
                                                                    tempo_series.loc[se_index]

        record_index_list_drop_dup = list(set(record_index_list))
        df_index_list = list(tempo_read_df.index.values)
        rest_index_list = list(set(df_index_list) - set(record_index_list_drop_dup))
        save_file1_path = os.path.join(args.save_path, '直接暴露' + '_' + str(index) + '.xlsx')
        save_df1 = tempo_read_df.loc[record_index_list_drop_dup]

        tempo_exp_identifier_items = tempo[tempo['exp_identifier'] == 1]['items'].tolist()
        tempo_qidentifier_items = tempo[tempo['qidentifier'] == 1]['items'].tolist()
        subsets = tempo_exp_identifier_items
        save_df1_drop_duplicates = save_df1.drop_duplicates(subset=subsets)

        if not save_df1_drop_duplicates.empty:
            df_Ab_index.append(index)
            # save_df1_drop_duplicates.to_excel(save_file1_path)
            direct_result.append(save_df1_drop_duplicates)
        else:
            df_Ab_risk_null.append(index)
        save_file2_path = os.path.join(args.save_path2, '无直接暴露' + '_' + str(index) + '.xlsx')
        save_df2 = tempo_read_df.loc[rest_index_list]
        # save_df2.to_excel(save_file2_path)

        info_mapping_dict = {}
        for info_type in ct.info_type_ls:
            info_mapping_dict[info_type] = []
        qid_length_map_dict = {}

        for test_key in ct.test_keys:
            id_records[index][test_key] = len(
                [x.strip() for x in save_df1_drop_duplicates[test_key].astype(str).dropna() if x.strip() != ''])
        for tempo_qidentifier_item in tempo_qidentifier_items + tempo_exp_identifier_items:
            qid_exposed_length = len(
                [x.strip() for x in save_df1_drop_duplicates[tempo_qidentifier_item].astype(str).dropna() if
                 x.strip() != ''])
            id_records[index][tempo_qidentifier_item] = qid_exposed_length

            for i in range(len(ct.class_keyword_list)):
                tempo_keyword_list = ct.class_keyword_list[i]
                for keyword in tempo_keyword_list:
                    if keyword in tempo_qidentifier_item:
                        info_mapping_dict[ct.info_type_ls[i]].append(tempo_qidentifier_item)
                        qid_length_map_dict[tempo_qidentifier_item] = qid_exposed_length

        for info_type in ct.info_type_ls:
            tempo_info_qid_ls = info_mapping_dict[info_type]
            qid_length_ls = []
            exp_length_ls = []

            try:
                tempo_info_exp_ls = ct.exp_info_mapping_dict[info_type]
                for tempo_info_exp in tempo_info_exp_ls:
                    exp_length_ls.append(id_records[index][tempo_info_exp])
                if exp_length_ls:
                    max_exp_length = max(exp_length_ls)
                else:
                    max_exp_length = 0
            except:
                max_exp_length = 0

            for tempo_info_qid in tempo_info_qid_ls:
                qid_length_ls.append(qid_length_map_dict[tempo_info_qid])

            if qid_length_ls:
                max_qid_length = max(qid_length_ls)
            else:
                max_qid_length = 0

            id_records[index][info_type] = max(max_exp_length, max_qid_length)

    return id_records, direct_result


def relabel_risk_calculation(df, exp_index_list):
    """
    重标识风险计算
    :param exp_index_list:
    :return:
    """
    df_filecode_list = df['filecode'].unique().tolist()
    deid_name_dict = {}
    for df_index in df_filecode_list:
        df_slice = df[df['filecode'] == df_index]
        df_slice_deid = df_slice[df_slice['deidentified'] == 1]
        deid_name = df_slice_deid[df_slice_deid['name_checked'] > 0.2]['items']
        if deid_name.isnull().all():
            continue
        deid_name_dict[df_index] = deid_name

    deid_name_dict_keys = deid_name_dict.keys()

    null_series = pd.Series([])
    res = {}
    for df_index in df_filecode_list:

        id_test = False
        no_id_test = False
        df_slice = df[df['filecode'] == df_index]
        qid_items = df_slice[df_slice['qidentifier'] == 1]['items'].tolist()
        id_items = \
            df_slice[df_slice['deidentified'] == 0][df_slice[df_slice['deidentified'] == 0]['exp_identifier'] == 1][
                'items'].tolist()
        deid_items = df_slice[df_slice['deidentified'] == 1]['items'].tolist()
        if df_index in exp_index_list:
            id_test = True
            try:
                file_path = os.path.join(args.save_path2, '无直接暴露' + '_' + str(df_index) + '.xlsx')
                df_tempo_read = pd.read_excel(file_path).drop(['Unnamed: 0'], axis=1)
            except:
                continue
            target_items = deid_items + id_items

        else:
            no_id_test = True
            tempo_df_list = []
            # TODO 这个注释可能会有问题：待优化
            # try:
            #     tempo_file_list = eval(df_slice['files_list'].iloc[0])
            # except:
            #     tempo_file_list = df_slice['files_list'].iloc[0]
            # df_tempo_read = pd.concat(
            #     [pd.concat(list(pd.read_excel(os.path.join(path, tempo_file), sheet_name=None).values()), axis=0) for
            #      tempo_file in tempo_file_list]).reset_index().drop(['index'], axis=1)

            target_items = deid_items
        # ---------------------------------------------------------------------------------------------------------------------------------------
        df_tempo_read = df_tempo_read.replace(['无数据'], [None]).replace(['无'], [None])
        new_columns = [x.replace('\n', '').replace('\r', '').replace('_x000D_', '') for x in
                       df_tempo_read.columns.tolist()]
        df_tempo_read.columns = new_columns
        deid_series_list = dp.dataset_to_series(df_tempo_read[deid_items])
        split_deid_se_list = []
        split_deid_column_name_list = []

        multiple_names = False
        if df_index in deid_name_dict_keys:
            deid_name_items_se = deid_name_dict[df_index]
            if len(deid_name_items_se) >= 3:
                multiple_names = True

        if deid_series_list:
            for deid_series in deid_series_list:
                if deid_series.replace('*', '').replace(' ', '').replace('无数据', '').replace([''], None).replace(['空'],
                                                                                                                None).isnull().all():
                    continue
                else:
                    split_res_list = []
                    for x in deid_series:
                        x = str(x)
                        split_res = dp.deid_split(x)
                        if split_res:
                            split_res_list.append(str(split_res))
                        else:
                            split_res_list.append('')
                    if split_res_list and not pd.Series(split_res_list).replace('', np.nan).isnull().all():
                        column_name = deid_series.name + '_剩余信息'
                        split_deid_column_name_list.append(column_name)
                        split_deid_se_list.append(
                            pd.Series(split_res_list, index=deid_series.index.values, name=column_name))
            if split_deid_se_list:
                for se in split_deid_se_list:
                    df_tempo_read = pd.concat([df_tempo_read, se], axis=1)

        id_series_list = dp.dataset_to_series(df_tempo_read[id_items])
        id_column_name_list = []
        if id_series_list:
            for id_series in id_series_list:
                if id_series.replace(' ', np.nan).isnull().all():
                    continue
                else:
                    id_column_name = id_series.name + '_标识符'
                    new_id_series = id_series.copy()
                    new_id_series.name = id_column_name
                    df_tempo_read = pd.concat([df_tempo_read, new_id_series], axis=1)
                    id_column_name_list.append(id_column_name)
        df_tempo_read = df_tempo_read.dropna(how='all', subset=target_items)
        if id_test:
            equ_class_items = split_deid_column_name_list + qid_items + id_column_name_list
        if no_id_test:
            equ_class_items = split_deid_column_name_list + qid_items

        # ---------------------------------------------------------------------------------------------------------------------------------------
        if equ_class_items:
            if multiple_names:
                continue
            tempo_df_reid = df_tempo_read.fillna('').groupby(equ_class_items, as_index=False).count()
            reid_fre_1_dict = {}
            save = False
            for target_item in (target_items):
                new_target_item = target_item.replace('/', '&')
                least_frequency = tempo_df_reid[target_item][tempo_df_reid[target_item] != 0].min()
                if least_frequency is np.nan:
                    continue
                number = (tempo_df_reid[target_item] == least_frequency).sum()
                risk = 1 / least_frequency
                res[(df_index, target_item)] = {'least_frequency': least_frequency, 'risk': risk, 'number': number,
                                                'qidset': equ_class_items,
                                                'qidset_len': len(equ_class_items)}
                if least_frequency == 1:
                    if not save:
                        save_file3_path = os.path.join(args.save_path3, str(df_index) + '.xlsx')
                        tempo_df_reid[tempo_df_reid[target_item] == least_frequency][equ_class_items].to_excel(
                            save_file3_path)
                        save = True
                        continue

    tempo_row_list = []
    for res_i in list(res.items()):
        key, value = res_i
        filecode_index, item_name = key
        value['qidset'] = str(value['qidset'])
        value['item'] = item_name
        tempo_row = pd.DataFrame(value, index=[filecode_index])
        tempo_row_list.append(tempo_row)
    reid_res_df = pd.concat(tempo_row_list, axis=0).reset_index()

    reid_res_sort_drop_duplicate_df = reid_res_df.sort_values(
        by=['risk'], ascending=[False]
    ).drop_duplicates(
        subset=['index', 'risk', 'qidset'], keep='first'
    ).set_index('index')

    reid_res_index = reid_res_sort_drop_duplicate_df.index.values

    df_no_exp_at_all_index = []
    df_no_exp_at_all_in_reid_index = []
    for tempo_filecode in df_filecode_list:
        if df[df['filecode'] == tempo_filecode]['exp_identifier'].sum() == 0:
            df_no_exp_at_all_index.append(tempo_filecode)
            if tempo_filecode in reid_res_index:
                df_no_exp_at_all_in_reid_index.append(tempo_filecode)
    no_exp_at_all_risk_null = list(set(df_no_exp_at_all_index) - set(df_no_exp_at_all_in_reid_index))

    df_exp_index = []
    for tempo_filecode in df_filecode_list:
        if df[df['filecode'] == tempo_filecode]['exp_identifier'].sum() >= 1:
            df_exp_index.append(tempo_filecode)

    df_Ab_index = df_exp_index
    df_Ab_in_reid_index = []
    for Ab_index in df_Ab_index:
        if Ab_index in reid_res_index:
            df_Ab_in_reid_index.append(Ab_index)

    all_no_exp_at_all_in_reid_index = "" if df_no_exp_at_all_in_reid_index != [""] \
        else "个人信息未直接披露数据集有：{}".format(df_no_exp_at_all_in_reid_index)
    print(all_no_exp_at_all_in_reid_index)

    reid_res_sort_drop_duplicate_df['数据集类别'] = ''
    reid_res_sort_drop_duplicate_df['数据集类别'].loc[df_Ab_in_reid_index] = \
        reid_res_sort_drop_duplicate_df['数据集类别'].loc[df_Ab_in_reid_index].apply(lambda x: '数据集Ab类')

    reid_res_sort_drop_duplicate_df['数据集类别'].loc[df_no_exp_at_all_in_reid_index] = \
        reid_res_sort_drop_duplicate_df['数据集类别'].loc[df_no_exp_at_all_in_reid_index].apply(lambda x: '数据集B类')

    return df_exp_index, df_no_exp_at_all_in_reid_index, reid_res_sort_drop_duplicate_df


def statistical_exposure(id_records, path='Intermediate_data/assessment/信息直接披露涉及数据集.xlsx'):
    """
    统计直接暴露数据信息披露情况，用于可视化
    :return:
    """
    id_records_dict = {}
    id_records_dict['filecode'] = list(id_records.keys())
    for record in ct.test_keys:
        id_records_dict[record] = [x[record] for x in list(id_records.values())]
    for info_type in ct.info_type_ls:
        id_records_dict[info_type] = [x[info_type] for x in list(id_records.values())]
    id_records_raw_df = pd.DataFrame(id_records_dict)

    for i in range(len(ct.classes)):
        info_type = ct.info_type_ls[i]
        cls = ct.classes[i]
        id_records_raw_df[cls] = (id_records_raw_df[info_type] > 0).astype(int)

    if os.path.exists(path):
        try:
            original_data = pd.read_excel(path).drop(['Unnamed: 0'], axis=1)
        except:
            original_data = pd.read_excel(path)
        res = pd.concat([original_data, id_records_raw_df], ignore_index=True).drop_duplicates()
        res.to_excel(path)
    else:
        id_records_raw_df.to_excel(path)


def run(df_final, table_data, table_name):
    df, df_groupped_sort, df_exp, exp_index_list, record_dict, record_tuple_list = read_data(df_final)

    # 这个是识别的结果
    id_records, direct_result = data_split(df_exp, table_name, exp_index_list, record_dict, record_tuple_list,
                                           table_data)
    statistical_exposure(id_records)

    id_records_dict = {}
    id_records_dict['filecode'] = list(id_records.keys())
    for record in ct.test_keys:
        id_records_dict[record] = [x[record] for x in list(id_records.values())]

    id_records_raw_df = pd.DataFrame(id_records_dict)

    plt.rcParams['font.sans-serif'] = ['SimHei']
    fig, ax = plt.subplots(figsize=(9, 7))
    sns.despine(fig)
    sns.barplot(data=id_records_raw_df, palette='Accent', alpha=0.9, edgecolor=".3", linewidth=.5)
    ax.set_ylabel('number', fontsize=9)
    ax.bar_label(ax.containers[0])
    ax.set_xlabel('Dataset Personal Information Disclosure', fontsize=9)

    if len(direct_result) > 0:
        direct_result[0][ct.keys] = direct_result[0][ct.keys].applymap(lambda x: ";".join(set(x.split(";")[:-1])))

        return direct_result[0], fig
    else:
        return pd.DataFrame([0], columns=['no result']), fig

import os
import warnings
import numpy as np
import pandas as pd
from Constants.path import args
import Constants.data_processing as dp
from Constants import tool as tl, constant as ct

warnings.filterwarnings("ignore")
intermediate_path = 'Intermediate_data/association'
if not os.path.exists(intermediate_path):
    os.makedirs(intermediate_path)


def read_data(path1='Intermediate_data/per_info/标识符识别结果.xlsx'):

    df_privacy = pd.read_excel(path1).drop(['Unnamed: 0'], axis=1)

    file_search_ls = tl.walkFile(args.save_path3)

    filecode_ls = [x.split('.')[0] for x in file_search_ls]
    filena_search_index = pd.DataFrame({
        'filecode': filecode_ls,
        'filename': file_search_ls
    })

    exp_record_list = tl.walkFile(args.save_path)

    exp_filecode_index_dict = {}
    for exp_record_name in exp_record_list:
        tempo_filecode = exp_record_name.split('_')[1].split('.')[0]
        exp_filecode_index_dict[tempo_filecode] = exp_record_name
    exp_filecode_index_ls = list(exp_filecode_index_dict.keys())

    reid_filecode_index_dict = {}
    for reid_record_name in filena_search_index['filename']:
        tempo_filecode = reid_record_name.split('.')[0]
        reid_filecode_index_dict[tempo_filecode] = reid_record_name
    reid_filecode_index_ls = list(reid_filecode_index_dict.keys())

    return df_privacy, exp_filecode_index_ls, reid_filecode_index_ls, exp_filecode_index_dict, reid_filecode_index_dict


def make_dict_and_association(df, exp_filecode_index_ls, reid_filecode_index_ls, exp_filecode_index_dict, reid_filecode_index_dict):
    """
    下方代码为分别读取直接暴露数据集(exp)和重标识风险为1的存在去标识化标识符的待重标识的数据集(reid)，
    分别制作exp_dataset_uni_qid_value_search_dict（exp数据集中的准标识符字典，使用去重的准标识符字段值）
    和 reid_dataset_uni_qid_value_search_dict（reid数据集中的准标识符字典，使用去重的准标识符字段值）
    :return:
    """
    count = 0
    exp_dataset_uni_qid_value_search_dict = {}
    exp_df_search_dict = {}
    for exp_index in exp_filecode_index_ls:
        count += 1
        # print(str(round(count / (len(exp_filecode_index_ls) + len(reid_filecode_index_ls)) * 100, 2)), end='\r')
        rest = reid_filecode_index_ls.copy()
        if exp_index in rest:
            rest.remove(exp_index)
        exp_file_name = exp_filecode_index_dict[exp_index]
        exp_file_path = os.path.join(args.save_path, exp_file_name)
        exp_df = pd.read_excel(exp_file_path)
        exp_new_columns = [x.replace('\n', '').replace('\r', '').replace('_x000D_', '') for x in
                           exp_df.columns.tolist()]
        exp_df.columns = exp_new_columns
        exp_df_columns = exp_df.columns.tolist()
        exp_df_slice = df[df['filecode'] == exp_index]
        exp_qid_items = exp_df_slice[exp_df_slice['qidentifier'] == 1]['items'].tolist()
        exp_df_search_dict[exp_index] = exp_df

        exp_dataset_uni_qid_value_search_dict[exp_index] = {}
        for exp_qid_item in exp_qid_items:

            # 新增
            uni_qid_values_ls = []
            uni_qid_index_ls = []
            for x in exp_df[exp_qid_item].dropna().unique():
                uni_qid_value = dp.qid_process(x)
                uni_qid_values_ls.append(uni_qid_value)
                uni_qid_index_ls.append(exp_df[exp_qid_item][exp_df[exp_qid_item] == x].index.tolist())

            uni_qid_values_index_df = pd.DataFrame({
                'matched_qid_value': uni_qid_values_ls,
                'mapping_index': uni_qid_index_ls
            })
            exp_dataset_uni_qid_value_search_dict[exp_index][exp_qid_item] = uni_qid_values_index_df

    reid_dataset_uni_qid_value_search_dict = {}
    reid_df_search_dict = {}
    for reid_index in reid_filecode_index_ls:
        count += 1
        # print(str(round(count / (len(exp_filecode_index_ls) + len(reid_filecode_index_ls)) * 100, 2)), end='\r')
        reid_file_name = reid_filecode_index_dict[reid_index]
        # reid_file_path = os.path.join(ph.save_path3, reid_file_name)
        reid_file_path = os.path.join(args.save_path3, reid_file_name)
        reid_df = pd.read_excel(reid_file_path)
        reid_new_columns = [x.replace('\n', '').replace('\r', '').replace('_x000D_', '') for x in
                            reid_df.columns.tolist()]
        reid_df.columns = reid_new_columns
        reid_df_columns = reid_df.columns.tolist()
        reid_df_slice = df[df['filecode'] == reid_index]
        reid_qid_items = reid_df_slice[reid_df_slice['qidentifier'] == 1]['items'].tolist()
        reid_df_search_dict[reid_index] = reid_df

        reid_dataset_uni_qid_value_search_dict[reid_index] = {}
        for reid_qid_item in reid_qid_items:
            uni_qid_values_ls = []
            uni_qid_index_ls = []
            for y in reid_df[reid_qid_item].dropna().unique():
                uni_qid_value = dp.qid_process(y)
                uni_qid_values_ls.append(uni_qid_value)
                uni_qid_index_ls.append(reid_df[reid_qid_item][reid_df[reid_qid_item] == y].index.tolist())

            uni_qid_values_index_df = pd.DataFrame({
                'matched_qid_value': uni_qid_values_ls,
                'mapping_index': uni_qid_index_ls
            })
            reid_dataset_uni_qid_value_search_dict[reid_index][reid_qid_item] = uni_qid_values_index_df

    match_qid_search_dict = {}
    for exp_index in exp_filecode_index_ls:
        rest = reid_filecode_index_ls.copy()
        if exp_index in rest:
            rest.remove(exp_index)
        for reid_index in rest:
            match_qid_search_dict[(exp_index, reid_index)] = []

    exp_match_new_df = {}
    reid_match_new_df = {}
    exp_match_new_uni_split_chinese_value_dict = {}
    exp_match_new_uni_other_value_dict = {}
    exp_split_value_types_mapping_dict = {}

    reid_match_new_uni_split_chinese_value_dict = {}
    reid_match_new_uni_other_value_dict = {}

    split_value_index_search_dict = {}
    all_uni_split_chinese_value_dict = {}
    all_uni_split_other_value_dict = {}

    deid_value_index_search_dict = {}
    all_uni_deid_chinese_value_dict = {}
    all_uni_deid_other_value_dict = {}

    final_match_res = []
    # exp_deid_chinese_ls_res = []
    # exp_deid_other_ls_res = []

    count = 0
    exp_keys = ['phone_records', 'ID_records', 'bank_records', 'car_id_records', 'name_records']
    # for exp_index in ['43406']:
    for exp_index in exp_filecode_index_ls:
        rest = reid_filecode_index_ls.copy()
        if exp_index in rest:
            rest.remove(exp_index)
        current_exp_qid_value_search_dict = exp_dataset_uni_qid_value_search_dict[exp_index]
        tempo_exp_qid_keys = list(current_exp_qid_value_search_dict.keys())
        # 修改
        tempo_exp_qid_values = list(current_exp_qid_value_search_dict.values())
        exp_df = exp_df_search_dict[exp_index]

        exp_df_slice = df[df['filecode'] == exp_index]
        exp_id_items = list(exp_df_slice[
                                exp_df_slice['deidentified'] == 0
                                ][exp_df_slice[
                                      exp_df_slice['deidentified'] == 0
                                      ]['exp_identifier'] == 1]['items'])
        exp_new_columns = exp_df.columns.tolist()
        exp_qid_items = exp_df_slice[exp_df_slice['qidentifier'] == 1]['items'].tolist()
        exp_id_items = [exp_id_item for exp_id_item in exp_id_items if exp_id_item in exp_new_columns]

        #    for reid_index in ['43521']:
        for reid_index in rest:
            count += 1
            pair = (exp_index, reid_index)
            # print(str(round(count / (len(exp_filecode_index_ls) * len(rest)) * 100, 2)), end='\r')

            current_reid_qid_value_search_dict = reid_dataset_uni_qid_value_search_dict[reid_index]
            tempo_reid_qid_keys = list(current_reid_qid_value_search_dict.keys())
            tempo_reid_qid_values = list(current_reid_qid_value_search_dict.values())
            # 修改
            tempo_reid_qid_values = list(current_reid_qid_value_search_dict.values())
            reid_df = reid_df_search_dict[reid_index]
            reid_df_slice = df[df['filecode'] == reid_index]
            reid_deid_items = list(reid_df_slice[
                                       reid_df_slice['deidentified'] == 1
                                       ]['items'] + '_剩余信息')
            reid_new_columns = reid_df.columns.tolist()
            reid_qid_items = reid_df_slice[reid_df_slice['qidentifier'] == 1]['items'].tolist()
            reid_deid_items = [reid_deid_item for reid_deid_item in reid_deid_items if
                               reid_deid_item in reid_new_columns]
            for i in range(len(tempo_exp_qid_values)):
                tempo_exp_qid_ls = tempo_exp_qid_values[i]['matched_qid_value'].tolist()
                i_key = tempo_exp_qid_keys[i]
                for j in range(len(tempo_reid_qid_values)):
                    tempo_reid_qid_ls = tempo_reid_qid_values[j]['matched_qid_value'].tolist()
                    j_key = tempo_reid_qid_keys[j]
                    intersect = list(set(tempo_exp_qid_ls).intersection(tempo_reid_qid_ls))
                    match_test = False
                    if intersect:
                        se_inter = pd.Series(intersect)
                        if se_inter.dtype != 'object':
                            i_item_name_matched = 0
                            j_item_name_matched = 0
                            for i_string in i_key:
                                if i_string in j_key:
                                    i_item_name_matched += 1
                            for j_string in j_key:
                                if j_string in i_key:
                                    j_item_name_matched += 1
                            i_matched_rate = i_item_name_matched / len(i_key)
                            j_matched_rate = j_item_name_matched / len(j_key)

                            if i_matched_rate >= 0.2 or j_matched_rate >= 0.2:
                                match_test = True
                        elif se_inter.dtype == 'object':
                            match_test = True
                    if match_test:
                        match_qid_search_dict[pair].append({
                            'matched_qid_pair': (i_key, j_key),
                            'intersection': intersect})

            matched_qid_searched = match_qid_search_dict[(exp_index, reid_index)]
            if not matched_qid_searched:
                continue
            matched_qid_searched_df = pd.DataFrame(matched_qid_searched)

            exp_searched_qid_indexes_dict = {}
            reid_searched_qid_indexes_dict = {}
            matched_exp_qid_ls = []
            matched_reid_qid_ls = []

            for matched_qid_searched_df_i in range(len(matched_qid_searched_df)):
                matched_exp_qid, matched_reid_qid = matched_qid_searched_df.iloc[matched_qid_searched_df_i][
                    'matched_qid_pair']
                matched_exp_qid_ls.append(matched_exp_qid)
                matched_reid_qid_ls.append(matched_reid_qid)

                intersection = matched_qid_searched_df.iloc[matched_qid_searched_df_i]['intersection']
                search_i = tempo_exp_qid_keys.index(matched_exp_qid)
                tempo_exp_qid_values_slice = tempo_exp_qid_values[search_i]
                exp_mapping_index_i = dp.list_extend([tempo_exp_qid_values_slice[
                                                       tempo_exp_qid_values_slice['matched_qid_value'] == item][
                                                       'mapping_index'].iloc[0] for item in intersection])
                exp_searched_qid_indexes_dict[matched_qid_searched_df_i] = exp_mapping_index_i
                search_j = tempo_reid_qid_keys.index(matched_reid_qid)
                tempo_reid_qid_values_slice = tempo_reid_qid_values[search_j]
                reid_mapping_index_j = dp.list_extend([tempo_reid_qid_values_slice[
                                                        tempo_reid_qid_values_slice['matched_qid_value'] == item][
                                                        'mapping_index'].iloc[0] for item in intersection])
                reid_searched_qid_indexes_dict[matched_qid_searched_df_i] = reid_mapping_index_j
            exp_intersected_index = dp.list_intersect(list(exp_searched_qid_indexes_dict.values()))
            reid_intersected_index = dp.list_intersect(list(reid_searched_qid_indexes_dict.values()))
            new_exp_df = exp_df.loc[exp_intersected_index]
            new_reid_df = reid_df.loc[reid_intersected_index]
            exp_match_new_df[pair] = new_exp_df
            reid_match_new_df[pair] = new_reid_df

            if new_exp_df.empty or new_reid_df.empty:
                continue
            # ///////////////////////////////////////////////////////////////////////////
            all_split_chinese_value = []
            all_split_other_value = []
            split_value_index_search_dict[pair] = {}
            exp_split_value_types_mapping_dict[pair] = {}
            for target_item in exp_keys:
                split_value_location_dict = {}
                exp_df_target_value = new_exp_df[target_item].dropna()
                for x in exp_df_target_value.index:
                    target_value = exp_df_target_value.loc[x]
                    split_value = list(filter(None, target_value.split(';')))
                    split_value_location_dict[x] = split_value
                keys_location = list(split_value_location_dict.keys())
                values_split_value = list(split_value_location_dict.values())

                for i in values_split_value:
                    for j in i:
                        j = str(j)
                        if tl.contain_chinese(j):
                            all_split_chinese_value.append(j)
                        else:
                            all_split_other_value.append(j)
                        split_value_index_search_dict[pair][j] = []
                        exp_split_value_types_mapping_dict[pair][j] = target_item

                for i in range(len(keys_location)):
                    value_split_value = values_split_value[i]
                    key_location = keys_location[i]
                    for v in value_split_value:
                        v = str(v)
                        split_value_index_search_dict[pair][v].append(key_location)

            current_exp_search_dict = split_value_index_search_dict[pair]
            current_search_exp_split_chinese_uni_value = list(set(all_split_chinese_value))
            all_uni_split_chinese_value_dict[pair] = current_search_exp_split_chinese_uni_value
            current_search_exp_split_other_uni_value = list(set(all_split_other_value))
            all_uni_split_other_value_dict[pair] = current_search_exp_split_other_uni_value

            # ///////////////////////////////////////////////////////////////////////////
            all_deid_value = []
            all_deid_chinese_value = []
            all_deid_other_value = []
            deid_value_index_search_dict[pair] = {}

            for deid_target_item in reid_deid_items:
                deid_value_location_dict = {}
                deid_df_target_value = new_reid_df[deid_target_item].dropna()
                for x in deid_df_target_value.index:
                    deid_target_value = deid_df_target_value.loc[x]
                    deid_value_eval = eval(deid_target_value)
                    deid_value_location_dict[x] = deid_value_eval
                deid_keys_location = list(deid_value_location_dict.keys())
                values_deid_value = list(deid_value_location_dict.values())

                for list_i in values_deid_value:
                    str_list_i = str(list_i)
                    chinese_test = False
                    for seg in list_i:
                        if tl.contain_chinese(seg):
                            chinese_test = True
                            break
                    if chinese_test:
                        all_deid_chinese_value.append(str_list_i)
                    else:
                        all_deid_other_value.append(str_list_i)
                    all_deid_value.append(str_list_i)
                    deid_value_index_search_dict[pair][str_list_i] = []
                for i in range(len(deid_keys_location)):
                    value_deid_value = values_deid_value[i]
                    deid_key_location = deid_keys_location[i]
                    deid_value_index_search_dict[pair][str(value_deid_value)].append(deid_key_location)

            current_reid_search_dict = deid_value_index_search_dict[pair]
            current_search_reid_deid_chinese_uni_value = list(set(all_deid_chinese_value))
            all_uni_deid_chinese_value_dict[pair] = current_search_reid_deid_chinese_uni_value
            current_search_reid_deid_other_uni_value = list(set(all_deid_other_value))
            all_uni_deid_other_value_dict[pair] = current_search_reid_deid_other_uni_value

            matched_qid_item_names_ls = matched_qid_searched_df['matched_qid_pair'].tolist()
            for exp_value in current_search_exp_split_chinese_uni_value:
                for deid_value in current_search_reid_deid_chinese_uni_value:
                    deid_value_ls = eval(deid_value)
                    match_test = True
                    if deid_value_ls[0] != exp_value[:1]:
                        continue
                    if len(deid_value_ls) >= 2:
                        if len(exp_value) == 2:
                            continue
                        if deid_value_ls[-1] != exp_value[-1:]:
                            continue

                    for seg in deid_value_ls:
                        seg = str(seg)
                        if seg not in exp_value:
                            match_test = False
                            continue
                    if match_test:
                        exp_row_index_ay = np.array(current_exp_search_dict[exp_value])
                        exp_record_slice = new_exp_df.loc[exp_row_index_ay]
                        reid_row_index_ay = np.array(current_reid_search_dict[deid_value])
                        reid_record_slice = reid_df.loc[reid_row_index_ay]
                        exp_index_ls = exp_record_slice.index.tolist()
                        reid_index_ls = reid_record_slice.index.tolist()
                        matched_qid_values_ls = [exp_record_slice[x].tolist() for x in exp_qid_items]
                        # exp_title = df[df['filecode'] == exp_index]['title'].iloc[0]
                        # reid_title = df[df['filecode'] == reid_index]['title'].iloc[0]
                        # title_pair = (exp_title, reid_title)
                        final_match_res.append({
                            'index_pair': str(pair),
                            # 'title_pair': title_pair,
                            'exp_value': exp_value,
                            'reid_value': str(deid_value_ls),
                            'qid_match_counts': len(matched_qid_searched_df),
                            'exp_row_index': str(exp_index_ls),
                            'exp_row_index_length': len(exp_index_ls),
                            'reid_row_index': str(reid_index_ls),
                            'reid_row_index_length': len(reid_index_ls),
                            'matched_qid_values': str(matched_qid_values_ls),
                            'matched_qid_item_names': str(matched_qid_item_names_ls),
                            'exp_qid_item_names': str(exp_qid_items),
                            'reid_qid_item_names': str(reid_qid_items),
                        })

            for exp_value in current_search_exp_split_other_uni_value:
                for deid_value in current_search_reid_deid_other_uni_value:
                    deid_value_ls = eval(deid_value)
                    #                if len(reid_row_indexes)>=reid_thresh:
                    #                    continue
                    match_test = True
                    len_test = True
                    for seg in deid_value_ls:
                        seg = str(seg)
                        if len(seg) <= 2:
                            len_test = False
                            continue
                        if seg not in exp_value:
                            match_test = False
                            continue

                    if not (deid_value_ls[0] == exp_value[:len(deid_value_ls[0])] and deid_value_ls[-1] == exp_value[
                                                                                                           -len(
                                                                                                                   deid_value_ls[
                                                                                                                       -1]):]):
                        continue

                    if match_test and len_test:
                        exp_row_index_ay = np.array(current_exp_search_dict[exp_value])
                        exp_record_slice = new_exp_df.loc[exp_row_index_ay]
                        reid_row_index_ay = np.array(current_reid_search_dict[deid_value])
                        reid_record_slice = reid_df.loc[reid_row_index_ay]
                        exp_index_ls = exp_record_slice.index.tolist()
                        reid_index_ls = reid_record_slice.index.tolist()
                        matched_qid_values_ls = [exp_record_slice[x].tolist() for x in exp_qid_items]
                        # exp_title = df[df['filecode'] == exp_index]['title'].iloc[0]
                        # reid_title = df[df['filecode'] == reid_index]['title'].iloc[0]
                        # title_pair = (exp_title, reid_title)
                        final_match_res.append({
                            'index_pair': str(pair),
                            # 'title_pair': str(title_pair),
                            'exp_value': exp_value,
                            'reid_value': str(deid_value_ls),
                            'qid_match_counts': len(matched_qid_searched_df),
                            'exp_row_index': str(exp_index_ls),
                            'exp_row_index_length': len(exp_index_ls),
                            'reid_row_index': str(reid_index_ls),
                            'reid_row_index_length': len(reid_index_ls),
                            'matched_qid_values': str(matched_qid_values_ls),
                            'matched_qid_item_names': str(matched_qid_item_names_ls),
                            'exp_qid_item_names': str(exp_qid_items),
                            'reid_qid_item_names': str(reid_qid_items),
                        })

    # print("final_match_res---------------------------------")
    # print(final_match_res)

    final_match_res_df = pd.DataFrame(final_match_res)
    for column in final_match_res_df.columns:
        try:
            final_match_res_df[column] = [eval(x) for x in final_match_res_df[column]]
        except:
            continue

    final_match_res_df['reid_row_index_length'] = [len(x) for x in final_match_res_df['reid_row_index']]
    final_match_res_df['reid_value_length'] = [len(x) for x in final_match_res_df['reid_value']]
    final_match_res_df['all_qid_length'] = final_match_res_df['qid_match_counts'] + final_match_res_df[
        'reid_value_length']
    final_match_res_df['exp_qid_length'] = [len(x) for x in final_match_res_df['exp_qid_item_names']]
    final_match_res_df['reid_qid_length'] = [len(x) for x in final_match_res_df['reid_qid_item_names']]
    final_match_res_df['more_info'] = final_match_res_df['exp_qid_length'] + final_match_res_df['reid_qid_length'] - 2 * \
                                      final_match_res_df['qid_match_counts']
    final_match_res_df['risk'] = [1 / x for x in final_match_res_df['reid_row_index_length']]
    more_qid_ls = []
    process_index = []
    for row_index in final_match_res_df.index:
        exp_qid_item_names = final_match_res_df.loc[row_index]['exp_qid_item_names'].copy()
        reid_qid_item_names = final_match_res_df.loc[row_index]['reid_qid_item_names'].copy()

        for i in final_match_res_df.loc[row_index]['matched_qid_item_names']:
            exp, reid = i
            try:
                exp_qid_item_names.remove(exp)
                reid_qid_item_names.remove(reid)
            except:
                try:
                    reid_qid_item_names.remove(reid)
                except:
                    continue
        more_qid_ls.append(exp_qid_item_names + reid_qid_item_names)
        try:
            exp_value = [str(x) for x in eval(final_match_res_df.loc[row_index]['exp_value'])]
        except:
            exp_value = str(final_match_res_df.loc[row_index]['exp_value'])
        reid_value = final_match_res_df.loc[row_index]['reid_value']

        if type(exp_value) is list:
            match_test = False
            for i in exp_value:
                tempo_match_test = True
                for j in reid_value:
                    if j in i:
                        i = i.replace(j, '')
                    else:
                        tempo_match_test = False

                if tempo_match_test:
                    match_test = True
            if match_test:
                process_index.append(row_index)
        else:
            no_match_test = False
            tempo_exp_value = exp_value
            for j in reid_value:
                if j in tempo_exp_value:
                    tempo_exp_value = tempo_exp_value.replace(j, '')
                else:
                    no_match_test = True
            if not no_match_test:
                process_index.append(row_index)

    final_match_res_df['all_qid'] = [list(set(x)) for x in
                                     final_match_res_df['exp_qid_item_names'] + final_match_res_df[
                                         'reid_qid_item_names']]
    final_match_res_df['more_qid'] = more_qid_ls

    association_results = final_match_res_df.loc[process_index]
    association_results.to_excel(os.path.join(intermediate_path, '关联结果.xlsx'))
    os.path.join(intermediate_path, '关联结果.xlsx')
    return association_results, exp_split_value_types_mapping_dict


def prepare_data(final_match_res_df2, exp_split_value_types_mapping_dict):
    l_personal_information_0 = []
    l_personally_identifiable_1 = []
    l_personal_health_2 = []
    l_personal_education_work_3 = []
    l_personal_property_4 = []
    l_other_5 = []
    class_list = [l_personal_information_0, l_personally_identifiable_1, l_personal_health_2,
                  l_personal_education_work_3, l_personal_property_4, l_other_5]

    for row_index in final_match_res_df2.index:
        all_info = final_match_res_df2['all_qid'].loc[row_index]
        expitem = final_match_res_df2['exp_value'].loc[row_index]
        pair = final_match_res_df2['index_pair'].loc[row_index]
        all_info.append(exp_split_value_types_mapping_dict[pair][expitem])
        for info in all_info:
            for i in range(len(ct.class_keyword_list1)):
                tempo_list = []
                tempo_keyword_list = ct.class_keyword_list1[i]
                for keyword in tempo_keyword_list:
                    if keyword in info:
                        tempo_list.append(row_index)
                if tempo_list:
                    class_list[i].append(tempo_list)

    for i in range(len(class_list)):
        class_list[i] = dp.list_extend(class_list[i])

    for i in range(len(ct.classes)):
        class_name = ct.classes[i]
        final_match_res_df2[class_name] = 0
        tempo = []
        for j in class_list[i]:
            final_match_res_df2[class_name].loc[[j]] = final_match_res_df2[class_name].loc[[j]].apply(lambda x: 1)

    # print(Counter(final_match_res_df2['all_qid_length']))
    #
    # print("final_match_res_df2----------------------------------")
    # print(final_match_res_df2)

    tempo_df_ls = []
    for i in final_match_res_df2['all_qid_length'].unique():
        tempo_expose_values_qlength = [final_match_res_df2[final_match_res_df2['all_qid_length'] == i][class_i].sum()
                                       for class_i in ct.classes]
        tempo_df_i = pd.DataFrame({
            'value': tempo_expose_values_qlength,
            'type': ct.classes,
            'qid_length': i
        })
        tempo_df_ls.append(tempo_df_i)
    expose_df = pd.concat(tempo_df_ls, axis=0)

    risk_range = []
    for x in final_match_res_df2['risk']:
        if 0 < x <= 0.25:
            risk_range.append('0.01-0.25')
        elif 0.25 < x <= 0.5:
            risk_range.append('0.25-0.5')
        elif x == 1:
            risk_range.append('1')
    final_match_res_df2['确信度'] = risk_range

    final_match_res_df3 = final_match_res_df2.sort_values('risk', ascending=False)


    return expose_df, final_match_res_df2, final_match_res_df3


def run():

    df, exp_filecode_index_ls, reid_filecode_index_ls, exp_filecode_index_dict, reid_filecode_index_dict = read_data()

    # 关联匹配
    association_results, exp_split_value_types_mapping_dict = \
        make_dict_and_association(df, exp_filecode_index_ls, reid_filecode_index_ls, exp_filecode_index_dict, reid_filecode_index_dict)

    expose_df, final_match_res_df2, final_match_res_df3 = prepare_data(association_results, exp_split_value_types_mapping_dict)

    return expose_df, final_match_res_df2, final_match_res_df3



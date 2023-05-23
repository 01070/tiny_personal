import re
import os
import numpy as np
import pandas as pd
from functools import reduce
from collections import Counter
from Constants import text_LAC as lac, type_judgment as tj


def sort_file_list(tempo_file_list):
    sort_dict = {}
    for i in tempo_file_list:
        index = int(re.findall(r"[0-9]+.*(?=\.)", i)[0])
        sort_dict[index] = i
    tempo_sort_res = sorted(sort_dict.items())
    sort_res = []
    for tup in tempo_sort_res:
        key, value = tup
        sort_res.append(value)
    return sort_res


def dataset_to_series(df):
    """
    输入数据表返还Series列表集合,即将数据表拆分为column为元素单位的列表。
    """
    # df = df.replace(['无数据'], [None]).replace(['无'], [None]).replace(['空'], [None])
    se_list = []
    for c, ls in df.iteritems():
        se = pd.Series(ls)
        se = se.dropna()
        se_list.append(se)
    return se_list


def dataset_to_series_unique(df):
    se_list = []
    for c, ls in df.iteritems():
        se = pd.Series(ls)
        ay = se.dropna().unique()
        se = pd.Series(ay)
        se_list.append(se)
    return se_list


def get_risk(input_list, how='max'):
    freq_dict = {}
    if input_list:
        for x in input_list:
            freq_dict[x] = freq_dict.get(x, 0) + 1
        if how == 'max':
            min_values = np.min(list(freq_dict.values()))
            risk = 1 / min_values
        elif how == 'mean':
            values = list(freq_dict.values())
            prob = [1 / x for x in values]
            uni_input_list = list(set(input_list))
            risk = np.sum(prob) / len(uni_input_list)
    else:
        risk = 0
    return risk


def deid_split(text):
    pattern = re.compile(r'[*]+')
    p = pattern.split(text)
    split_text = list(filter(None, p))
    for text in split_text:
        if tj.is_chinese(text) == False and len(text) < 3:
            split_text.remove(text)
    return split_text


def deid_match_test(split_text, target_text):
    test = True
    if split_text:
        for element in split_text:
            if element not in target_text:
                test = False
    else:
        test = False
    return test


def averageLen(lst):
    lengths = [len(i) for i in lst]
    return 0 if len(lengths) == 0 else (float(sum(lengths)) / len(lengths))


def qid_match_test(qid_text, target_list):
    test = False
    if qid_text in target_list:
        test = True
    return test


def list_extend(lists):
    res = []
    for i in lists:
        for j in i:
            res.append(j)
    res = list(set(res))
    return res


def list_intersect(ls):
    return list(reduce(lambda x,y : set(x) & set(y), ls))


def df_data_processing(df_data):
    # df_data = df_data.replace(['无数据'],[None],['无'],[None],['空'],[None])
    # 新增
    df_data = df_data.dropna(subset=['title'])
    rows = [sort_file_list(sorted_row) for sorted_row in [row.split(';') for row in df_data['filenames']]]
    df_data['files_list'] = rows
    columns_rows = [row.split(';') for row in df_data['columns']]
    df_data['columns_list'] = columns_rows
    new_df_data_ls = []
    to_retain = ['title', 'domain', 'files_list']
    for row_index in range(len(df_data)):
        tempo_row = df_data.iloc[row_index]
        for item in tempo_row['columns_list']:
            tempo = pd.DataFrame(tempo_row[to_retain]).T
            tempo['item'] = item
            new_df_data_ls.append(tempo)
    new_df_data = pd.concat(new_df_data_ls, axis=0)
    return new_df_data


def query_and_tag(dir_path, df_data):
    '''
    字段值域、字符特征采用检查
    :param dir_path: 
    :param df_data: 
    :return: 
    '''
    index_list = df_data.index.unique()
    ratio_list = []
    dtype_list = []
    #    name_test_list = []
    len_list = []
    chichar_test_list = []
    engchar_test_list = []
    numchar_test_list = []
    other_test_list = []
    deidchar_test_list = []
    null_test_list = []
    for index in index_list:
        df_slice = df_data.loc[index]
        try:
            files = df_slice['files_list'].iloc[0]
        except:
            files = df_slice['files_list']
        tempo_df_read_ls = []
        for file in files:
            file_path = os.path.join(dir_path, file)
            tempo_df_read = pd.read_excel(file_path)
            tempo_df_read_ls.append(tempo_df_read)
        df_read = pd.concat(tempo_df_read_ls, axis=0)
        df_read = df_read.replace(['无数据'], [None]).replace(['无'], [None]).replace(['空'], [None])
        seriesList = dataset_to_series(df_read)
        for index in range(len(seriesList)):
            if seriesList[index].isnull().all():
                ratio_list.append(None)
                dtype_list.append('notype')
                chichar_test_list.append(None)
                engchar_test_list.append(None)
                numchar_test_list.append(None)
                other_test_list.append(None)
                len_list.append(None)
                deidchar_test_list.append(None)
                null_test_list.append(1)
                continue
            null_test_list.append(0)
            series = seriesList[index]
            dtype = str(series.dtype)
            if dtype == 'bool':
                dtype = 'object'
            dtype_list.append(dtype)
            # 取Series的唯一值
            uni_ls = series.dropna().unique().tolist()
            # 计算唯一取值比例
            num_uni_values = len(uni_ls)
            num_all_values = len(series)
            try:
                ratio = num_uni_values / num_all_values
            except:
                ratio = None
            ratio_list.append(ratio)
            if len(series) >= 10:
                sample_list = [value for value, counts in Counter(series).most_common(10)]
            if len(series) < 10:
                sample_list = [value for value, counts in Counter(series).most_common(len(series))]
            chichar_test = 0
            engchar_test = 0
            numchar_test = 0
            otherchar_test = 0
            deidchar_test = 0
            sample_len_list = []

            for i in sample_list:
                str_i = str(i)
                chichar_test, engchar_test, numchar_test, otherchar_test, deidchar_test = tj.contain_test(str_i,
                                                                                                          chichar_test,
                                                                                                          engchar_test,
                                                                                                          numchar_test,
                                                                                                          otherchar_test,
                                                                                                          deidchar_test)
                sample_len_list.append(len(str_i))
            len_list.append(np.mean(sample_len_list))
            chichar_test_list.append(chichar_test / len(sample_list))
            engchar_test_list.append(engchar_test / len(sample_list))
            numchar_test_list.append(numchar_test / len(sample_list))
            other_test_list.append(otherchar_test / len(sample_list))
            deidchar_test_list.append(deidchar_test / len(sample_list))
    df_data['ratio'] = ratio_list
    df_data['dtype'] = dtype_list
    #    df_data['name_checked'] = name_test_list
    df_data['chichar_test'] = chichar_test_list
    df_data['engchar_test'] = engchar_test_list
    df_data['numchar_test'] = numchar_test_list
    df_data['other_test'] = other_test_list
    df_data['deidchar_test'] = deidchar_test_list
    df_data['average_length'] = len_list
    df_data['null_test'] = null_test_list
    df_data = df_data.dropna()
    return df_data


def result(final):
    final2 = final.copy()
    to_seg_cols = ['title', 'domain', 'item']
    all_dict = {}
    for to_seg_col in to_seg_cols:
        new_dict = {}
        for x in final2[to_seg_col].unique():
            seg_res = lac.LAC_seg(x)
            new_dict[x] = str(seg_res)
        all_dict[to_seg_col] = new_dict

    final2['title_new'] = final2.title.apply(lambda x: all_dict['title'][x])
    final2['domain_new'] = final2.domain.apply(lambda x: all_dict['domain'][x])
    final2['item_new'] = final2.item.apply(lambda x: all_dict['item'][x])
    final3 = final2.fillna('空')

    return final3


def qid_process(string):
    if type(string) == str:
        string = string.replace('\t', '').replace('\n', '').replace('\r', '').replace('_x000D_', '')
        spe_chars = ['.','/','-','_']
        for spe_char in spe_chars:
            if spe_char in string:
                spl_res = string.split(spe_char)
                if len(spl_res)>=2:
                    string = '|'.join(spl_res[:2])
        return string
    else:
        return string


def show_values(axs, orient="v", space=.01,small_value = 0,special_value = [None]):
    def _single(ax):
        if orient == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height() + (p.get_height()*0.01)
                value = '{:.0f}'.format(p.get_height())
                if p.get_height()<=small_value:
                    continue
                if p.get_height() in special_value:
                    continue
                ax.text(_x, _y, value, ha="center")
        elif orient == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + float(space)
                _y = p.get_y() + p.get_height() - (p.get_height()*0.5)
                value = '{:.0f}'.format(p.get_width())
                if p.get_height()<=small_value:
                    continue
                if p.get_height() in special_value:
                    continue
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _single(ax)
    else:
        _single(axs)


def show_values_spe(axs, orient="v", space=.01,small_value = 0,special_value = [None], bin_range = None,bins=None):
    def _single(ax):
        if orient == "v":
            if bin_range:
                f_bin,l_bin = bin_range
                current_bin = f_bin
                for p in ax.patches[f_bin:l_bin]:
                    _x = p.get_x() + p.get_width() / 2
                    _y = p.get_y() + p.get_height() + (p.get_height()*0.01)
                    bin_index = np.mod(current_bin,bins)
                    value = '{:.0f}'.format(p.get_height()+ax.patches[bin_index].get_height()+ax.patches[bin_index+bins].get_height())
                    if p.get_height()<=small_value:
                        current_bin +=1
                        continue
                    if p.get_height() in special_value:
                        current_bin +=1
                        continue
                    current_bin +=1
                    ax.text(_x, _y, value, ha="center")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _single(ax)
    else:
        _single(axs)
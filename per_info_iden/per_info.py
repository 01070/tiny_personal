import re
import os
import time
import datetime
import copy
import hanlp
import pandas as pd
from Constants import bankcode, areacode, data_conversion as dc, text_LAC as tl, type_judgment as tj, \
    data_processing as dp
import warnings
from LAC import LAC
from stdnum import luhn

warnings.filterwarnings("ignore")


def ask_sample(dict_res, sample_len, random_state=None):
    all_keys = pd.Series(dict_res.keys())
    if len(all_keys) >= sample_len:
        samples = str(all_keys.sample(sample_len, random_state=random_state).tolist())
    else:
        samples = str(all_keys.sample(len(all_keys), random_state=random_state).tolist())
    return samples


def direct_phones(uni_ls=None, series=None):
    """
    针对先前被判定为纯数字的字段值，使用正则表达式，进行手机电话披露检测，返还包括披露数量、检测结果和采样值的字典集。
    """
    x = 0
    dict_res = {}
    sample = None
    extract = False
    if uni_ls is not None:
        r = r'(?<![0-9a-zA-Z\-])(?:\+?86)?1(?:(?:34[0-8])|(?:8\d{2})|(?:(?:[35][0-35-9]|4[14-9]|6[567]|7[0-8]|9[12389])\d))\d{7}(?![0-9a-zA-Z\-])'
        for i in uni_ls:
            num = re.findall(r, i)
            if num:
                extract = True
                dict_value_index = dc.index_map_list(series, i)
                for j in num:
                    dict_res[j] = dict_value_index
                    x += 1
        test_res = dc.test(x, 1, 1)
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
        return {'phones': x, 'phone_test': test_res, 'phone_extract': sample}, dict_res
    else:
        test_res = dc.test(x, 1, 1)
        return {'phones': x, 'phone_test': test_res, 'phone_extract': sample}, dict_res


def direct_email(uni_ls=None, series=None):
    """
    检测邮箱。
    """
    x = 0
    dict_res = {}
    sample = None
    extract = False
    if uni_ls is not None:
        r = r'\w+@[\da-z\.-]+\.(?:[a-z]{2,6}|[\u2E80-\u9FFF]{2,3})'
        for i in uni_ls:
            num = re.findall(r, i)
            if num:
                extract = True
                dict_value_index = dc.index_map_list(series, i)
                for j in num:
                    dict_res[j] = dict_value_index
                    x += 1
        test_res = dc.test(x, 1, 1)
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
    else:
        test_res = dc.test(x, 1, 1)
    return {'phones': x, 'phone_test': test_res, 'phone_extract': sample}, dict_res


def direct_ip(uni_ls=None, series=None):
    """
    检测ip地址。
    """
    x = 0
    dict_res = {}
    sample = None
    extract = False
    if uni_ls is not None:
        r = r"(((\d{1,2})|(1\d\d)|(2[0-4]\d)|(25[0-5]))\.){3}(((\d\d)|(1\d\d)|(2[0-4]\d)|\d|(25[0-5])))"
        for i in uni_ls:
            num = re.findall(r, i)
            if num:
                extract = True
                dict_value_index = dc.index_map_list(series, i)
                for j in num:
                    dict_res[j] = dict_value_index
                    x += 1
        test_res = dc.test(x, 1, 1)
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
    else:
        test_res = dc.test(x, 1, 1)
    return {'ip': x, 'ip_test': test_res, 'ip_extract': sample}, dict_res


def get_current_date():
    year = time.localtime(time.time())[0]
    today = datetime.datetime.now().strftime('%Y%m%d')
    return today, year


def validate_ID(ID, today, year):
    coeff = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    check = [1, 0, 'X', 9, 8, 7, 6, 5, 4, 3, 2]
    val_ID_test = False
    if int(ID[6:10]) in range(1900, year + 1):
        if int(ID[6:14]) <= int(today):
            try:
                time.strptime(ID[6:14], "%Y%m%d")
            except:
                return val_ID_test
            tmp = 0
            for i in range(0, 17):
                tmp = tmp + int(ID[i]) * coeff[i]
            mod = tmp % 11
            if str(check[mod]) == ID[-1]:
                val_ID_test = True
    return val_ID_test


def direct_ID(uni_se=None, series=None, today=None, year=None):
    """
    针对先前被判定为纯数字的字段值，使用正则表达式，进行身份证号码披露检测，返还包括披露数量、检测结果和采样值的字典集。
    """
    x = 0
    dict_res = {}
    sample = None
    extract = False
    if uni_se is not None:
        r = r'[1-9]\d{5}(?:18|19|(?:[23]\d))\d{2}(?:(?:0[1-9])|(?:10|11|12))(?:(?:[0-2][1-9])|10|20|30|31)\d{3}[0-9Xx]'
        for i in uni_se:
            num = re.findall(r, i)
            if num:

                for j in num:
                    val_ID_test = validate_ID(j, today, year)
                    areacode_test = j[:6] in areacode.areacode_dict
                    if val_ID_test and areacode_test:
                        extract = True
                        dict_value_index = dc.index_map_list(series, i)
                        dict_res[j] = dict_value_index
                        x += 1
                    else:
                        continue

        test_res = dc.test(x, 1, 1)
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
        return {'ID': x, 'ID_test': test_res, 'ID_extract': sample}, dict_res
    else:
        test_res = dc.test(x, 1, 1)
        return {'ID': x, 'ID_test': test_res, 'ID_extract': sample}, dict_res


def direct_bank(uni_ls=None, series=None, today=None, year=None):
    """
    针对先前被判定为纯数字的字段值，使用外部银行卡号码种类库，进行银行卡号码披露检测，返还包括披露数量、检测结果和采样值的字典集。
    """

    x = 0
    dict_res = {}
    sample = None
    extract = False
    if uni_ls is not None:
        for i in uni_ls:

            bank_pattern = re.compile(r"(?<![0-9a-zA-Z\-])[1-9](?:\d{11,18})(?![0-9a-zA-Z\-])")
            nums = re.findall(bank_pattern, i)
            for num in nums:

                if num and bankcode.card_type(num) != None and luhn.is_valid(num):
                    extract = True
                    dict_value_index = dc.index_map_list(series, i)
                    dict_res[num] = dict_value_index
                    x += 1
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
        test_res = dc.test(x, 1, 1)
        return {'bank': x, 'bank_test': test_res, 'bank_extract': sample}, dict_res
    else:
        test_res = dc.test(x, 1, 1)
        return {'bank': x, 'bank_test': test_res, 'bank_extract': sample}, dict_res


def car_id(uni_ls=None, series=None):
    """
    针对先前被判定为文本的字段值，使用正则表达式，进行车牌号码披露检测，返还包括披露数量、检测结果和采样值的字典集。
    车牌号码检测无法在分词结果中进行，只能直接判断。
    """
    x = 0
    dict_res = {}
    sample = None
    extract = False
    if uni_ls is not None:
        r = r'(?<![锅容管瓶梯起索游车]\d{2}(?=[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁台琼使领军北南成广沈济空海]{1}[A-Z]{1}[A-Z0-9]{4}(?:[A-Z0-9挂领学警港澳]{1}|[A-Z0-9]{2}\(\d{2}\))))[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁台琼使领军北南成广沈济空海]{1}[A-Z]{1}[A-Z0-9]{4}(?:[A-Z0-9挂领学警港澳]{1}|[A-Z0-9]{2})(?!\d)'
        pattern = re.compile(r)
        for i in uni_ls:
            result = pattern.findall(i)
            if result:
                extract = True
                dict_value_index = dc.index_map_list(series, i)
                for z in result:
                    dict_res[z] = dict_value_index
                    x += 1
        test_res = dc.test(x, 1, 1)
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
        return {'car_id': x, 'car_id_test': test_res, 'car_id_extract': sample}, dict_res
    else:
        test_res = dc.test(x, 1, 1)
        return {'car_id': x, 'car_id_test': test_res, 'car_id_extract': sample}, dict_res


def names_han(lac_result=None, uni_ls=None, series=None):
    """
    对于判定为文本的字段，均进行姓名检测，输入词性标注和主体识别结果，返还包括披露数量、检测结果和采样值的字典集。
    """
    dict_res = {}
    sample = None
    extract = False
    num_exposed = 0
    if lac_result is not None:
        for index, text in enumerate(lac_result):
            target_text = " ".join([item[0] for item in text if item[1] == "PERSON"])
            for temp in target_text.split(" "):
                if 1 < len(temp) <= 4 and tj.is_chinese2(temp):
                    extract = True
                    raw = uni_ls[index]
                    dict_value_index = dc.index_map_list(series, raw)
                    dict_res[temp] = dict_value_index
                    num_exposed += 1
        test_res = dc.test(num_exposed, 1, 1)
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
        return {'name': num_exposed, 'name_test': test_res, 'name_extract': sample}, dict_res
    else:
        test_res = dc.test(num_exposed, 1, 1)
        return {'name': num_exposed, 'name_test': test_res, 'name_extract': sample}, dict_res


def names(lac_result=None, uni_ls=None, series=None):
    """
    对于判定为文本的字段，均进行姓名检测，输入词性标注和主体识别结果，返还包括披露数量、检测结果和采样值的字典集。
    """
    dict_res = {}
    sample = None
    extract = False
    num_exposed = 0
    if lac_result is not None:
        for i in range(len(lac_result)):
            if 'PER' in lac_result[i][1]:
                target_text = lac_result[i][0][lac_result[i][1].index('PER')]
                if 1 < len(target_text) <= 4 and tj.is_chinese2(target_text):
                    extract = True
                    raw = uni_ls[i]
                    dict_value_index = dc.index_map_list(series, raw)
                    dict_res[target_text] = dict_value_index
                    num_exposed += 1
        test_res = dc.test(num_exposed, 1, 1)
        if extract:
            sample = ask_sample(dict_res, 5, random_state=42)
        return {'name': num_exposed, 'name_test': test_res, 'name_extract': sample}, dict_res
    else:
        test_res = dc.test(num_exposed, 1, 1)
        return {'name': num_exposed, 'name_test': test_res, 'name_extract': sample}, dict_res


def deid_names(lac_name_ratio_res=None):
    dict_res = {}
    return {'name': lac_name_ratio_res, 'name_test': 0}, dict_res


def query_all_fields(df, de_id_threshold=0.1, str_len_threshold=200, ner_func="lac", recall_mode=False):
    """
    遍历读取查询数据集全部字段有针对性的完成检测。默认去标识化比例阈值为0.5，默认字段记录字符长度阈值为200.
    """
    random_state = 1024
    today, year = get_current_date()

    seriesList = dp.dataset_to_series(df)
    num_uni_values_list = []
    num_all_values_list = []
    ratio_list = []
    test_res_list = []
    test_records_list = []
    sample_list = []
    column_names = dc.dataset_get_columns(df)
    #########################################################
    names_like_columns_list = []
    addr_like_columns_list = []
    addr_like_columns_regx = r'(住[所址]$)|(户籍)|(家庭地址)|(人员?地址)|(住所地址)|(社区(区划)?$)'
    names_like_columns_regx = r'(names?)|(者$)|(姓 ?名)|(人名)|(人?员名?[称字单]?[^\u4E00-\u9FA5]*$)|(.*人[^\u4E00-\u9FA5]*$)|(曾用名)|(法人代表[^\u4E00-\u9FA5]*$)'
    if ner_func != "lac":
        ner_func = hanlp.pipeline().append(hanlp.load('FINE_ELECTRA_SMALL_ZH'),
                                           output_key='tok') \
            .append(hanlp.load(hanlp.pretrained.ner.MSRA_NER_ELECTRA_SMALL_ZH))

    def name_func():
        if ner_func == "lac":
            uni_ls_old = copy.copy(uni_ls)
            lac_res = tl.LAC_test(uni_ls, lac_lac)
            name_res, name_records = names(lac_res, uni_ls_old, series)

        else:
            han_res = ner_func(uni_ls)
            name_res, name_records = names_han(han_res, uni_ls, series)
        return name_res, name_records

    for index, column in enumerate(column_names):
        names_like_column = re.search(names_like_columns_regx, column)
        addr_like_column = re.search(addr_like_columns_regx, column)

        if names_like_column is not None:
            names_like_columns_list.append(index)
        if addr_like_column is not None:
            addr_like_columns_list.append(index)
    # for column in names_like_columns_list:
    #     lac_lac = LAC(mode='lac')
    #     # TODO 平均长度判断
    #     res = lac_lac.run(sample_df[column].tolist())
    #     ratio = len(list(filter(lambda x: 1 if "PER" in x[1] else 0, res))) / 100
    #     if ratio > 0.9:
    #         names_columns.append(column)
    # names_df = table_data[names_columns]

    #########################################################

    # 剔除全为空值的Series
    lac_lac = LAC(mode='lac')
    for index in range(len(seriesList)):
        # 遍历全部的字段
        if seriesList[index].isnull().all():
            # 忽略为空的序列
            ratio_list.append(None)
            sample_list.append(None)
            test_res_list.append(None)
            test_records_list.append(None)
            continue
        # 进行Series纯数字检测并将Series字符串化。
        digitest, series = dc.digit_test(seriesList[index])

        # 计算字段记录的长度
        # try:
        #     item_str_length = series.apply(lambda x: np.mean([len(w) for w in x.split()])).mean()
        # except:
        #     item_str_length = 0

        # 取Series的唯一值并进行去标识化程度检测
        uni_ls = series.dropna().unique().tolist()
        de_id_ratio = dc.de_id_test(series)

        # 计算唯一取值比例
        num_uni_values = len(uni_ls)
        num_uni_values_list.append(num_uni_values)
        num_all_values = len(series)
        num_all_values_list.append(num_all_values)
        try:
            ratio = num_uni_values / num_all_values
        except:
            ratio = None
        ratio_list.append(ratio)

        try:
            sample = seriesList[index].dropna().sample(1, random_state=random_state).iloc[0]
        except:
            sample = None
        # 默认忽略含有“序号”和“时间”的字段
        # default_skip_words = ['序号', '主键ID', '时间', '地址', '住址', '日期', '企业名', '单位名', '活动名']
        # default_skip_full_words = ['单位', '企业', '辖区']

        skip = False

        # 现阶段仍忽略字段平均文本长度超过阈值的字段。默认的阈值为200，200字符长度非常长了。
        # 这里后续再想方法如何高效地处理长文本，可能弹出采样？让用户决定是否进行忽略。
        # if item_str_length >= str_len_threshold:
        #     skip = True

        # 默认的忽略关键词检测，若为真则直接忽略
        if skip:
            # 忽略关键词检测，若为真则略过
            sample_list.append(sample)
            phone_res, phone_records = direct_phones()
            ID_res, ID_records = direct_ID()
            bank_res, bank_records = direct_bank()
            name_res, name_records = names()
            car_id_res, car_id_records = car_id()

            de_id_test_ = 0 if de_id_ratio <= de_id_threshold else 1
            test_res_list.append({'de_id_res': {'de_id_ratio': de_id_ratio, 'de_id_test': de_id_test_},
                                  'phones_res': phone_res,
                                  'ID_res': ID_res,
                                  'bank_res': bank_res,
                                  'name_res': name_res,
                                  'car_id_res': car_id_res})

            test_records_list.append({'phone_records': phone_records,
                                      'ID_records': ID_records,
                                      'bank_records': bank_records,
                                      'car_id_records': car_id_records,
                                      'name_records': name_records})
            continue

        if digitest == 1:
            # 数字检测，若字符串为纯数字则不进行分词，直接进行手机号、身份证号和银行卡号的检测
            # 若为非纯数字，则进行分词和所有检测
            phone_res, phone_records = direct_phones(uni_ls, series)
            ID_res, ID_records = direct_ID(uni_ls, series, today, year)
            bank_res, bank_records = direct_bank(uni_ls, series, today, year)
            car_id_res, car_id_records = car_id()
            name_res, name_records = names()
            test_res_list.append(
                {'de_id_res': {'de_id_ratio': de_id_ratio, 'de_id_test': 0}, 'phones_res': phone_res, 'ID_res': ID_res,
                 'bank_res': bank_res, 'name_res': name_res, 'car_id_res': car_id_res, })
            test_records_list.append({'phone_records': phone_records,
                                      'ID_records': ID_records,
                                      'bank_records': bank_records,
                                      'car_id_records': car_id_records,
                                      'name_records': name_records})
        else:
            seg_condition_test = tj.list_seg_condition(uni_ls)

            if de_id_ratio <= de_id_threshold:
                # 判定字段去标识化程度，小于阈值则被认为没有进行去标识化或去标识化程度较低，需要进行常规的所有检测。

                car_id_res, car_id_records = car_id(uni_ls, series)
                ID_res, ID_records = direct_ID(uni_ls, series, today, year)
                phone_res, phone_records = direct_phones(uni_ls, series)
                bank_res, bank_records = direct_bank(uni_ls, series)
                if seg_condition_test:
                    # 分词条件判断检测，若字段值中出现中文、句号和空格及下划线，则分词条件判断为真
                    # recall_mode
                    # 如果字段名中有地址、且人名识别率不高，则跳过


                    if recall_mode and index in names_like_columns_list and len(seriesList[index]) > 100:
                        lac_res = []
                        series_len = seriesList[index].dropna().apply(lambda x: len(str(x)))
                        series_mean_len = series_len.mean()
                        series_std_len = series_len.std()
                        # 结构化检测
                        if 2 < series_mean_len < 4 and series_std_len < 1:
                            sample = seriesList[index].sample(100).fillna(" ")
                            res = lac_lac.run(sample.tolist())
                            ratio = len(list(filter(lambda x: 1 if "PER" in x[1] else 0, res))) / 100
                            if ratio > 0.9:
                                for name in uni_ls:
                                    if 2 <= len(name) <= 4:
                                        lac_res.append([[name], ['PER']])
                                    else:
                                        lac_res.append([[name], ['None']])

                                name_res, name_records = names(lac_res, uni_ls, series)
                            else:
                                name_res, name_records = name_func()
                        else:
                            name_res, name_records = name_func()

                    else:
                        name_res, name_records = name_func()


                else:
                    name_res, name_records = names()

                test_res_list.append(
                    {'de_id_res': {'de_id_ratio': de_id_ratio, 'de_id_test': 0}, 'phones_res': phone_res,
                     'ID_res': ID_res,
                     'bank_res': bank_res, 'name_res': name_res, 'car_id_res': car_id_res})
                test_records_list.append({'phone_records': phone_records,
                                          'ID_records': ID_records,
                                          'bank_records': bank_records,
                                          'car_id_records': car_id_records,
                                          'name_records': name_records})
            else:
                # 若去标识化程度大于阈值，则可以判定字段进行了较高程度的去标识化
                # 由于分词中的姓名检测对于去标识化的字段效果较差，故略过姓名检测

                # 将*替换为数字以试探性进行手机电话的检测
                # 同时也可以试探性进行去标识化姓名的检测
                # 以便后续在重标识分析中判定去标识化字段的所属信息类型
                car_id_res, car_id_records = car_id()
                ID_res, ID_records = direct_ID(uni_ls, series, today, year)
                phone_res, phone_records = direct_phones(uni_ls, series)
                bank_res, bank_records = direct_bank(uni_ls, series)

                if seg_condition_test:
                    # TODO
                    # seg_res = tl.LAC_seg_test(uni_ls, lac_seg)
                    name_res, name_records = name_func()
                    # name_res, name_records = names()

                else:
                    name_res, name_records = names()

                test_res_list.append(
                    {'de_id_res': {'de_id_ratio': de_id_ratio, 'de_id_test': 1}, 'phones_res': phone_res,
                     'ID_res': ID_res,
                     'bank_res': bank_res, 'name_res': name_res, 'car_id_res': car_id_res})

                test_records_list.append({'phone_records': phone_records,
                                          'ID_records': ID_records,
                                          'bank_records': bank_records,
                                          'car_id_records': car_id_records,
                                          'name_records': name_records})
        sample_list.append(sample)

    return [column_names, ratio_list, sample_list, test_res_list], test_records_list, column_names


def run(data_dire, df_p='../data/Intermediate_data/per_info/df.pkl'):
    if os.path.exists(df_p):
        df_privacy = pd.read_pickle(df_p)
        return df_privacy

    # 读取目录
    df_data = pd.read_excel(data_dire)

    # 文件排序
    try:
        rows = [dp.sort_file_list(sorted_row) for sorted_row in [row.split(';') for row in df_data['filenames']]]
        df_data['files_list'] = rows
    except:
        try:
            df_data['files_list'] = [eval(x) for x in df_data['files_list']]
        except:
            df_data['files_list'] = [[x] for x in df_data['files_list']]
    try:
        df_privacy = df_data[df_data['check'] == 1]
    except:
        df_privacy = df_data[df_data['privacy'] == 1]

    df_privacy['filecode'] = df_privacy['filecode'].astype(int)

    if not os.path.exists(os.path.dirname(df_p)):
        os.makedirs(os.path.dirname(df_p))

    pd.to_pickle(df_privacy, df_p)

    return df_privacy

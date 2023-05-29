import numpy as np
import re


def dataset_get_columns(df):
    """
    输入数据表返还数据列名，去除换行符，以匹配的重置数据集目录中的字段名。
    """
    df.columns = df.columns.map(lambda x: x.replace('\n', '').replace('\r', ''))
    columnsList = df.columns.tolist()
    return columnsList


def index_map_list(str_se, value):
    index_list = str_se[str_se == value].index.tolist()
    return index_list


def test(number, requirement, tag):
    """
    输入变量阈值检测，输入变量number与阈值进行大小比较，返还输入变量tag。
    """
    test = 0
    if number >= requirement:
        test = tag
    else:
        test = 0
    return test


def digit_test(se):
    """
    Series取值特征纯数字检测，返还包含检测结果和字符串转化的序列的元组。
    出于后续正则表达式和NLP的目的，需要将Series的数据类型进行字符串转换。
    """
    test_res = 0
    if se.dtype == 'int64' or se.dtype == 'float64':
        test_res = 1
        if se.dtype == 'float64':
            se = se.astype(np.int64)
        str_se = se.astype(str).str.replace(' ', '')
    else:
        str_se = se.astype(str).str.replace(' ', '')
    return test_res, str_se


def de_id_test(str_se):
    """
    序列去标识化检测，输入文本化转化的Series，返还'*'在Series取值中出现的比例。
    """
    counts = 0
    len_str_se = len(str_se)
    for value in str_se:
        if '*' in value:
            counts += 1
    try:
        ratio = counts / len_str_se
    except:
        ratio = None
    return ratio


def deid_split(text):
    pattern = re.compile(r'[*]+')
    p = pattern.split(text)
    split_text = str(list(filter(None, p)))

    return split_text



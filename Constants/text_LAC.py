import numpy as np
from LAC import LAC


def LAC_test(uni_ls, lac_lac):
    """
    批量LAC文本分割，将输入的字符串化的Series进行文本分割，分割语义元素及中文汉字组合、数字与英文字符存储在列表。
    如文本“Nanhu南湖实验室123456”会被分割为['Nanhu', '南湖', '实验室', '123456']
    """
    res = lac_lac.run(uni_ls)
    return res

# TODO
def LAC_seg_test(uni_ls, lac_seg):
    """
    批量LAC文本分割，将输入的字符串化的Series进行文本分割，分割语义元素及中文汉字组合、数字与英文字符存储在列表。
    如文本“Nanhu南湖实验室123456”会被分割为['Nanhu', '南湖', '实验室', '123456']
    """
    res = lac_seg.run(uni_ls)
    return res


def LAC_deid_name_test(lac_res):
    """
    批量LAC文本词性标注和实体识别，识别文本中的成分，以便后续检测姓名。
    """
    x = 0
    all_counts = len(lac_res)
    for test_res in lac_res:
        if 'PER' in test_res[1]:
            x += 1
    try:
        res = x / all_counts
    except:
        res = 0
    return res


def LAC_top_rank(text):
    lac = LAC(mode='rank')
    if len(text) > 8:
        if 0 < text.count('-') <= 1:
            text, lac_res = split_rank_test(text, '-')
        else:
            lac_res = lac.run(text)
    else:
        lac_res = lac.run(text)
    rank_3_res_list = []
    rank_2_res_list = []
    rank_3_count = 0
    rank_2_count = 0
    for rank_index in range(len(lac_res[2])):
        if lac_res[1][rank_index] != 'LOC' and lac_res[1][rank_index] != 'ORG':
            if lac_res[2][rank_index] == 3:
                rank_3_res_list.append(lac_res[0][rank_index])
                rank_3_count += 1
                if rank_3_count == 3:
                    break

    while rank_3_count < 3:
        rank_3_res_list.append('无')
        rank_3_count += 1

    return rank_3_res_list


def LAC_seg(string):
    lac = LAC(mode='seg')
    res = lac.run(string)
    return res


def split_rank_test(text, split_keyword):
    lac = LAC(mode='rank')
    text_split_ls = text.split(split_keyword)
    rank_sum_ls = []
    lac_res_ls = []
    for text_split in text_split_ls:
        tempo_lac_res = lac.run(text_split)
        rank_res = []
        for rank_index in range(len(tempo_lac_res[2])):
            if tempo_lac_res[1][rank_index] != 'LOC' or tempo_lac_res[1][rank_index] != 'ORG':
                rank_res.append(tempo_lac_res[2][rank_index])
        rank_sum_ls.append(np.sum(rank_res))
        lac_res_ls.append(tempo_lac_res)
    if rank_sum_ls[0] > rank_sum_ls[1]:
        text = text_split_ls[0]
        lac_res = lac_res_ls[0]
    else:
        text = text_split_ls[1]
        lac_res = lac_res_ls[1]
    return text, lac_res


def LAC_list_name_test(str_ls, thresh = 0.1):
    """
    批量LAC文本词性标注和实体识别，识别文本中的成分，以便后续检测姓名。
    """
    lac = LAC(mode='lac')
    test_res = lac.run(str_ls)
    x = 0
    all_counts = len(test_res)
    for test in test_res:
        if 'PER' in test[1]:
            x+=1
    try:
        res = x/all_counts
    except:
        res = 0
    if res >=thresh:
        return True
    else:
        return False
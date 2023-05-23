import string as strg


def is_chinese(char):
    if u'\u4e00' < char < u'\u9fff':
        return True
    return False


def is_chinese2(string):
    """
    输入文本值，检测是否为中文字符。
    """
    for chart in string:
        if chart < u'\u4e00' or chart > u'\u9fff':
            return False
    return True


# 列表中中文字符的情况
def list_seg_condition(str_ls):
    test_res = False
    for element in str_ls:
        for char in element:
            if is_chinese2(char) or ' 'in char or '，' in char or '。'in char or '；' in char or '_' in char:
                test_res = True
                break
    return test_res


def is_english(char):
    if char in strg.ascii_lowercase + strg.ascii_uppercase:
        return True
    return False


def is_digit(char):
    if char.isdigit():
        return True
    return False


def is_deid(char):
    if char == '*':
        return True
    return False


def contain_test(string, chichar_test, engchar_test, numchar_test, otherchar_test, deidchar_test):
    chichar = False
    engchar = False
    numchar = False
    deidchar = False
    otherchar = False

    for char in string:
        if is_chinese(char):
            chichar = True
        if is_english(char):
            engchar = True
        if is_digit(char):
            numchar = True
        if is_deid(char):
            deidchar = True
        if not is_chinese(char) and not is_english(char) and not is_digit(char) and not is_deid(char):
            otherchar = True
    if chichar:
        chichar_test += 1
    if engchar:
        engchar_test += 1
    if numchar:
        numchar_test += 1
    if otherchar:
        otherchar_test += 1
    if deidchar:
        deidchar_test += 1

    return chichar_test, engchar_test, numchar_test, otherchar_test, deidchar_test
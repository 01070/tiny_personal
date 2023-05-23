import os


def to_input(items_input=None):
    items_input = input().replace('\'', '').replace('，', ',').replace("[", "").replace("]", "")
    if items_input == "":
        return []
    items_to_drop = []
    try:
        if ' ' in items_input:
            items_to_drop = items_input.replace(',', ' ').split(' ')
            items_to_drop = list(filter(None, items_to_drop))
        elif ',' in items_input:
            items_to_drop = items_input.replace(' ', ',').split(',')
            items_to_drop = list(filter(None, items_to_drop))
        else:
            items_to_drop.append(items_input)
    except:
        items_to_drop = []
    return items_to_drop


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


def getFlist(file_dir):
    """
    获取文件夹中所有文件的路径
    """
    for root, dirs, files in os.walk(file_dir):
        return files


def walkFile(file):
    f_list = []
    for root, dirs, files in os.walk(file):

        # root 表示当前正在访问的文件夹路径
        # dirs 表示该文件夹下的子目录名list
        # files 表示该文件夹下的文件list

        # 遍历文件

        for f in files:
            f_list.append(f)
    return f_list


def contain_chinese(string):
    """
    输入文本值，检测是否为中文字符。
    """
    for chart in string:
        if u'\u4e00'<chart <u'\u9fff':
            return True
    return False

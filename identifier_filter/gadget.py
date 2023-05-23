
def deal(expose, field, val, index_list, df_privacy, df_checked):

    items_name = expose['items'].unique().tolist()
    if len(items_name) == 0:
        return None

    # 排查并标记剩余的字段
    to_drop_index_list = []
    try:
        res = []
        for i in to_drop_index_list:
            for j in i:
                res.append(j)
        diff = list(set(index_list) - set(res))
        df_privacy[field + '_checked'][res] = df_privacy[field + '_checked'][res].apply(lambda x: 0)

        if field == 'phones':
            df_checked[field[:-1] + '_identifier'][diff] = df_checked[field[:-1] + '_identifier'][diff].apply(lambda x: 1)
        else:
            df_checked[field + '_identifier'][diff] = df_checked[field + '_identifier'][diff].apply(lambda x: 1)
    except TypeError:
        return None

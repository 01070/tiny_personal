import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import Constants.data_processing as dp
from Constants import tool as tl, constant as ct

sns.set_theme(style="whitegrid", font_scale=1.2, font="simhei")
sns.set_theme(style='whitegrid')
rc = {'font.sans-serif': 'SimHei',
      'axes.unicode_minus': False}
sns.set(context='notebook', style='ticks', rc=rc)

image_path = 'output_data/association_data_image'
if not os.path.exists(image_path):
    os.makedirs(image_path)


def prepare(expose_df):
    g = sns.catplot(
        data=expose_df, kind="bar",
        x="type", y="value", hue="qid_length",
        palette="Reds", alpha=0.9, height=6, edgecolor=".3", linewidth=.5, aspect=1.6
    )
    g.despine(left=True)
    plt.xticks(rotation=60)
    g.set_xlabels("")
    g.set_ylabels("涉及人数")
    ax1 = g.axes
    dp.show_values(ax1, "v", space=0.01, small_value=-1)
    g.legend.set_title("准标识符匹配数量")
    sns.move_legend(g, "upper left", bbox_to_anchor=(0.75, 0.9), frameon=True)
    # plt.savefig(os.path.join(image_path, '匹配个人信息涉及人数.png'), dpi=300, bbox_inches='tight')

    fig = g.figure
    return fig


def per_info_expan(final_match_res_df3):
    fig, ax = plt.subplots(figsize=(10, 7))
    sns.despine(fig)
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))

    ax.set_xlabel("关联后个人信息扩充量")
    ax.set_ylabel("涉及人数")

    axv = sns.histplot(data=final_match_res_df3, palette='Reds_r', bins=20, x="more_info", hue="确信度", multiple="stack")

    for rect in axv.patches:
        # 获取矩形条的高度和位置
        height = rect.get_height()
        width = rect.get_width()
        x = rect.get_x()
        y = rect.get_y()

        # 添加文本注释
        axv.text(x + width / 2, height + y + 0.5, f"{height:.0f}", ha='center', va='top', fontsize=10)

    return fig


def draw(final_match_res_df2):
    df_cata_exp = pd.read_excel('Intermediate_data/assessment/信息直接披露涉及数据集.xlsx')
    df_cata_exp = df_cata_exp.drop_duplicates(subset=['filecode'])

    value_classes_ls = [x + '暴露' for x in ct.classes]

    df_cata_exp_link = df_cata_exp.copy()
    df_cata_exp_link['type'] = '单个数据集情况'

    final_match_res_df2['exp_filecode_index'] = [exp_filecode_index for exp_filecode_index, reid_filecode_index in
                                                 final_match_res_df2['index_pair']]
    final_match_res_df2['reid_filecode_index'] = [reid_filecode_index for exp_filecode_index, reid_filecode_index in
                                                  final_match_res_df2['index_pair']]

    # 数据集数

    viz_datasets_exp_ls = []
    viz_datasets_reid_ls = []
    new_exp_filecode_index_ls = final_match_res_df2['exp_filecode_index'].unique().tolist()
    new_reid_filecode_index_ls = final_match_res_df2['reid_filecode_index'].unique().tolist()

    for exp_index in new_exp_filecode_index_ls:
        tempo_linked_rows = final_match_res_df2[final_match_res_df2['exp_filecode_index'] == exp_index]
        exp_se = df_cata_exp[df_cata_exp['filecode'] == exp_index]
        exp_se2 = exp_se.copy()
        exp_se2['type'] = '平台综合情况'

        for class_i in range(len(ct.classes)):
            info_test = False
            tempo_linked_rows_info_sum = tempo_linked_rows[ct.classes[class_i]].sum()
            if tempo_linked_rows_info_sum > 0:
                info_test = True
            original_value = exp_se[ct.classes[class_i]].iloc[0]
            if original_value == 0 and info_test:
                exp_se2[ct.classes[class_i]] = 1

        if exp_index in new_reid_filecode_index_ls:
            tempo_linked_rows2 = final_match_res_df2[final_match_res_df2['reid_filecode_index'] == exp_index]
            exp_se3 = exp_se2.copy()
            for class_i in range(len(ct.classes)):
                info_test = False
                tempo_linked_rows_info_sum2 = tempo_linked_rows2[ct.classes[class_i]].sum()
                if tempo_linked_rows_info_sum2 > 0:
                    info_test = True
                original_value = exp_se2[ct.classes[class_i]].iloc[0]
                if original_value == 0 and info_test:
                    exp_se3[ct.classes[class_i]] = 1
            new_reid_filecode_index_ls.remove(exp_index)
            viz_datasets_exp_ls.append(exp_se3)
        else:
            viz_datasets_exp_ls.append(exp_se2)

    for reid_index in new_reid_filecode_index_ls:
        tempo_linked_rows = final_match_res_df2[final_match_res_df2['reid_filecode_index'] == reid_index]
        reid_se = df_cata_exp[df_cata_exp['filecode'] == reid_index]
        reid_se2 = reid_se.copy()
        if reid_se2.empty:
            continue
        reid_se2['type'] = '平台综合情况'

        for class_i in range(len(ct.classes)):
            info_test = False
            tempo_linked_rows_info_sum = tempo_linked_rows[ct.classes[class_i]].sum()
            if tempo_linked_rows_info_sum > 0:
                info_test = True
            # 标题唯一才能使用
            original_value = reid_se[ct.classes[class_i]].iloc[0]
            if original_value == 0 and info_test:
                reid_se2[ct.classes[class_i]] = 1
        viz_datasets_reid_ls.append(reid_se2)

    viz_datasets_num_after = pd.concat(viz_datasets_exp_ls + viz_datasets_reid_ls, axis=0)

    df_cata_exp_dataset_link_after = df_cata_exp.copy()
    df_cata_exp_dataset_link_after['type'] = '平台综合情况'

    df_cata_exp_dataset_link_after.loc[viz_datasets_num_after.index] = viz_datasets_num_after

    dataset_num_original = [df_cata_exp_link[class_i].sum() for class_i in ct.classes]
    dataset_num_after = [df_cata_exp_dataset_link_after[class_i].sum() for class_i in ct.classes]
    dataset_num_df = pd.concat([pd.DataFrame({
        'value': dataset_num_original,
        'type': ct.classes,
        'link': '单个数据集情况'
    }), pd.DataFrame({
        'value': dataset_num_after,
        'type': ct.classes,
        'link': '平台综合情况'
    })], axis=0)

    g = sns.catplot(
        data=dataset_num_df, kind="bar",
        x="type", y="value", hue="link",
        palette="Reds", alpha=0.9, height=6, edgecolor=".3", linewidth=.5, aspect=1.6
    )
    g.despine(left=True)
    plt.xticks(rotation=60)
    g.set_xlabels("")
    g.set_ylabels("涉及数据集数量")
    ax1 = g.axes
    dp.show_values(ax1, "v", space=0.01, small_value=-1)
    g.legend.set_title("个人信息直接披露汇总")
    sns.move_legend(g, "upper left", bbox_to_anchor=(0.75, 0.9), frameon=True)
    plt.savefig(os.path.join(image_path, '关联匹配个人信息涉及数据集增量.png'), dpi=300, bbox_inches='tight')

    viz_res_value_num_ls = []
    for exp_index in final_match_res_df2['exp_filecode_index'].unique():
        tempo_linked_rows = final_match_res_df2[final_match_res_df2['exp_filecode_index'] == exp_index]
        exp_se = df_cata_exp[df_cata_exp['filecode'] == exp_index]
        exp_se2 = exp_se.copy()
        exp_se2['type'] = '平台综合情况'

        for class_i in range(len(ct.classes)):
            info_test = False
            tempo_linked_rows_info_sum = tempo_linked_rows[ct.classes[class_i]].sum()
            original_exp_num_value = exp_se[value_classes_ls[class_i]].iloc[0]
            if tempo_linked_rows_info_sum > 0:
                info_test = True
            # 标题唯一才能使用
            original_value = exp_se[value_classes_ls[class_i]].iloc[0]
            if original_value == 0 and info_test:
                exp_se2[ct.classes[class_i]] = 1
                exp_se2[value_classes_ls[class_i]] = tempo_linked_rows_info_sum
        viz_res_value_num_ls.append(exp_se2)

    viz_value_num_after = pd.concat(viz_res_value_num_ls, axis=0)

    df_cata_exp_value_link_after = df_cata_exp.copy()
    df_cata_exp_value_link_after['type'] = '平台综合情况'

    value_num_original = [df_cata_exp_link[value_class_i].sum() for value_class_i in value_classes_ls]
    value_num_after = [df_cata_exp_value_link_after[value_class_i].sum() for value_class_i in value_classes_ls]
    value_num_df = pd.concat([pd.DataFrame({
        'value': value_num_original,
        'type': ct.classes,
        'link': '单个数据集情况'
    }), pd.DataFrame({
        'value': value_num_after,
        'type': ct.classes,
        'link': '平台综合情况'
    })], axis=0)

    g2 = sns.catplot(
        data=value_num_df, kind="bar",
        x="type", y="value", hue="link",
        palette="Reds", alpha=0.9, height=6, edgecolor=".3", linewidth=.5, aspect=1.6
    )
    g2.despine(left=True)
    plt.xticks(rotation=60)
    g2.set_xlabels("")
    g2.set_ylabels("涉及人数")
    ax1 = g2.axes
    dp.show_values(ax1, "v", space=0.01, small_value=-1)
    g2.legend.set_title("个人信息直接披露汇总")
    sns.move_legend(g, "upper left", bbox_to_anchor=(0.75, 0.9), frameon=True)
    # plt.savefig(os.path.join(image_path, '关联匹配个人信息涉及人数增量.png'), dpi=300, bbox_inches='tight')

    return g.figure, g2.figure


def per_info_disclose(df_class, df_number_class):
    """
    数据集个人信息直接披露情况
    :param df_class:
    :param df_number_class:
    :return:
    """

    fig1, ax = plt.subplots(figsize=(15, 10))
    sns.despine(fig1)
    sns.barplot(data=df_class, palette='Accent', alpha=0.9, edgecolor=".3", linewidth=.5)
    ax.set_ylabel('数据集个人信息直接披露数量', fontsize=12)
    ax.set_xlabel('个人信息类型', fontsize=12)
    ax.bar_label(ax.containers[0])
    plt.title('数据集个人信息直接披露情况', fontsize=16, fontweight='bold')

    fig2, ax = plt.subplots(figsize=(15, 10))
    sns.despine(fig2)
    sns.barplot(data=df_number_class, palette='Accent', alpha=0.9, edgecolor=".3", linewidth=.5)
    ax.set_ylabel('数据集个人信息直接披露涉及人数', fontsize=12)
    ax.set_xlabel('个人信息类型', fontsize=12)
    ax.bar_label(ax.containers[0])
    plt.title('数据集个人信息直接披露涉及人数情况', fontsize=16, fontweight='bold')

    return fig1, fig2
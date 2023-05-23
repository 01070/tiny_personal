categorical_cols = ['dtype']
minmax_col = ['average_length']
text_pool_cols = ['title_new', 'domain_new', 'item_new', 'rank_res']
numeric_cols = ['ratio', 'chichar_test', 'engchar_test', 'numchar_test', 'other_test', 'deidchar_test',
                'average_length']

# 常见的准标识符表示的关键词
default_qid_keywords = ['工作', '学历', '学位', '专业', '学校', '毕业', '学科', '职务', '性别', '出生', '户口类型', '单位', '家庭地址', '户口', '年龄',
                        '生日', '残疾', '镇', '市', '省', 'AZAA0002', '救助类别', '政治面貌', '民族', '名族', '企业名称', '身体状况',
                        '籍贯', '居住', '从业', '文化程度', '户籍', '最美称号级别', '百姓所属乡镇', '受理人员', '毕业',
                        '补助', '辖区', '所属村社区', '所属工会', '婚姻', '宗教', '生理', '邮政编码', '机构名称', '岗位', '住所',
                        '专利权人地址', '医院', '科室', '住址', '所属网格', '所属社区', '所属组织']

# 若通过上述关键词筛选出的字段名中包含下面关键词，则予以舍弃。
default_to_drop_keyword_list = ['人口', '序号', '简介', '审计单位编码', '图片', '总数', '批文号', '明确材料', '管理机构编码', '承运', '承租', '出租',
                                '系统ID', '价格单位', '管理单位性质', '金额']

to_retain1 = ['exp_identifier', 'deidentified', 'qidentifier']
# to_retain2 = ['filecode', 'title', 'items', 'ratio', 'sample', 'phones_checked',
#               'ID_checked', 'bank_checked', 'name_checked', 'car_id_checked']

to_retain2 = ['filecode', 'items', 'ratio', 'sample', 'phones_checked',
              'ID_checked', 'bank_checked', 'name_checked', 'car_id_checked']

origin = ['phones', 'ID', 'bank', 'name', 'car_id']
# columns_to_retain = ['title', 'items', 'ratio', 'sample', 'de_id_test']
columns_to_retain = ['items', 'ratio', 'sample', 'de_id_test']
check = ['phones_checked', 'ID_checked', 'bank_checked', 'name_checked', 'car_id_checked']
identifier = ['name_identifier', 'phone_identifier', 'ID_identifier', 'bank_identifier', 'car_id_identifier']

# ？？这里需要人工手动输入？ 从哪些里面筛选呢？JL
# 暂时，这里的筛选根据上方所有准标识符类来做，下面是常见的关键词，这个部分人工参与仍较多，后续依然可以用机器学习来使用嘉兴市作为大样本集
# 来完成各种信息类的分类，如果有额外需要加入的关键词，直接在下方列表里加，就暂时不添加eval了，可能会混淆

k_personal_information_0 = ['AZZAA002', '城市', '居住地', '村', '性别', '民族', '名族', '出生', '乡镇', '籍贯', '户口', '户籍', '年龄',
              '地址', '镇编码', '邮政编码', '住址', '所属网格', '所属社区', '所属组织', '区县市', '所属省份', '市级编码', '所属镇街', '居住状况',
              '户籍所在镇街', '所在省']
k_personally_identifiable_1 = []
k_personal_health_2 = ['残疾', 'BHIX0013', '救助', '补贴', '生理', '疾病']
k_personal_education_work_3 = ['单位', '企业', '职务', '学历', '学位', '专业', '学校', '文化程度', '从业', '工作', '市优类型', '机构', '医院', '科室', '学段',
                '任教', '班级', '毕业', '成绩', '警种', '警员', '比赛名称', '项目', '市场主体', '学段', '班级']
k_personal_property_4 = ['房产', '不动产']
k_other_5 = ['宗教', '教别', '教派', '婚姻状况', '问题类型']
class_keyword_list = [k_personal_information_0, k_personally_identifiable_1, k_personal_health_2,
                      k_personal_education_work_3, k_personal_property_4, k_other_5]

test_keys = ['phone_records', 'ID_records', 'bank_records', 'name_records', 'car_id_records']
checked_keys = ['phones_checked', 'ID_checked', 'bank_checked', 'name_checked', 'car_id_checked']
info_type_ls = ['个人基本信息暴露', '个人身份信息暴露', '个人健康生理信息暴露',
                '个人教育工作信息暴露', '个人财产信息暴露', '其他信息暴露']
exp_info_mapping_dict = {
    '个人基本信息暴露': ['name_records', 'phone_records'],
    '个人身份信息暴露': ['ID_records', 'car_id_records'],
    '个人财产信息暴露': ['bank_records']
}

classes = ['个人基本信息', '个人身份信息', '个人健康生理信息', '个人教育工作信息', '个人财产信息', '其他信息']
classes_number = ['个人基本信息人数', '个人身份信息人数', '个人健康生理信息人数', '个人教育工作信息人数', '个人财产信息人数', '其他信息人数']

keys = ['phone_records', 'ID_records', 'bank_records', 'car_id_records', 'name_records']


k_personal_information_01 = ['AZZAA002', '城市', '居住地', '村', '性别', '民族', '名族', '出生', '乡镇', '籍贯', '户口', '户籍', '年龄',
              '地址', '镇编码', '邮政编码', '住址', '所属网格', '所属社区', '所属组织', '区县市', '所属省份', '市级编码', '所属镇街', '居住状况',
              '户籍所在镇街', '所在省'] + ['name', 'phone']
k_personally_identifiable_11 = [] + ['ID', 'id']
k_personal_health_21 = ['残疾', 'BHIX0013', '救助', '生理', '疾病']
k_personal_education_work_31 = ['单位', '企业', '职务', '学历', '学位', '专业', '学校', '文化程度', '从业', '工作', '市优类型', '机构', '医院', '科室', '学段',
                '任教', '班级', '毕业', '成绩', '警种', '警员', '比赛名称', '项目', '市场主体', '学段', '班级']
k_personal_property_41 = ['房产', '不动产'] + ['bank']
k_other_51 = ['宗教', '婚姻状况', '问题类型']
class_keyword_list1 = [k_personal_information_01, k_personally_identifiable_11, k_personal_health_21,
                       k_personal_education_work_31, k_personal_property_41, k_other_51]

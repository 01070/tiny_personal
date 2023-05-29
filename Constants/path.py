import argparse
import os

parser = argparse.ArgumentParser()

# 第一步利用决策树分类的路径
parser.add_argument("--dir_path",            default='../data/input_data/嘉兴全部数据集', type=str, help='数据集分类模型的训练数据')
parser.add_argument("--raw_path",            default='../data/input_data/嘉兴市数据集重制目录.xlsx', type=str, help='对应的训练数据集目录')
parser.add_argument("--data_dir",            default='../data/input_data/嘉兴市数据集demo-个人信息披露检查.xlsx', type=str, help='数据集分类模型的训练标签')
parser.add_argument("--identifier_path",     default='../data/input_data/训练数据.xlsx', type=str, help='嘉兴市数据集demo-标识符识别结果')

# 保存的中间结果
parser.add_argument("--model_path",  default='../data/Intermediate_data/ml_data/model_clf.dat', type=str, help='训练好的决策树模型')
parser.add_argument("--save_path",   default='Intermediate_data/assessment/隐私数据集直接披露Aa', type=str, help='隐私数据集直接披露Aa保存路径')
parser.add_argument("--save_path2",  default='Intermediate_data/assessment/隐私数据集直接披露Ab', type=str, help='隐私数据集直接披露Ab保存路径')
parser.add_argument("--save_path3",  default='Intermediate_data/assessment/重标识风险为1数据项', type=str, help='重标识风险为1数据项保存路径')

# 后续的步骤所用到的路径
parser.add_argument("--target_city_dataset", default='../data/input_data/嘉兴市数据集demo', type=str, help='待预测的数据集')
parser.add_argument("--target_city",         default='../data/input_data/嘉兴市数据集重置目录demo.xlsx', type=str, help='待预测的数据集目录')
parser.add_argument("--Dis_ins_data",        default='../data/input_data/嘉兴市数据集-个人信息披露检查predict.xlsx', type=str, help='生成的个人信息披露检查结果')

parser.add_argument("--database", default='per_info', type=str)
parser.add_argument("--table", default='jiaxing', type=str)


args = parser.parse_args()

# if not os.path.exists(args.save_path):
#     os.makedirs(args.save_path)
# if not os.path.exists(args.save_path2):
#     os.makedirs(args.save_path2)
# if not os.path.exists(args.save_path3):
#     os.makedirs(args.save_path3)

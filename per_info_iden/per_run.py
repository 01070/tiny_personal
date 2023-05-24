import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from . import identification as iden



def run(table_data, table_name, ner_func="lac", recall_mode=False):

    # table_data = table_data.drop_duplicates()
    output_dataframe_list_concat = iden.info_identification(table_data, table_name, ner_func=ner_func, recall_mode=recall_mode)
    return output_dataframe_list_concat
    # # 解码结果
    # final_data = de.run()
    #
    # df_final = fr.run(final_data)  # 标识符识别结果
    # id_records_raw_df, fig = at.run(df_final, table_data, table_name)  # 最终结果
    #
    # if len(id_records_raw_df) == 1:
    #     return id_records_raw_df, fig
    #
    # va = id_records_raw_df.iloc[:, -5:]
    # va.insert(5, '', '')
    # vb = id_records_raw_df.iloc[:, :-5]
    # vc = pd.concat([va, vb], axis=1)
    # vc.reset_index(drop=True, inplace=True)

    # return vc, fig



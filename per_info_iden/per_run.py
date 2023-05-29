import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from . import identification as iden



def run(table_data, recall_mode=False):

    # table_data = table_data.drop_duplicates()
    output_dataframe_list_concat = iden.info_identification(table_data, recall_mode=recall_mode)
    return output_dataframe_list_concat


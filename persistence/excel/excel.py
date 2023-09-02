import datetime
import logging
from typing import List
import pandas as pd




class CreatExcel(object):
    def __init__(self):
        pass

    def write(self, excel_path: str, name: str, dict_list: List[dict]) -> str:
        if not len(dict_list):
            logging.info("[to excel]: there is no data")
            return '0'
        time_now = datetime.date.today()
        time_now_to_string = time_now.strftime('%Y_%m_%d_')
        df = pd.DataFrame(dict_list)
        df.to_excel(excel_path + "/" + time_now_to_string + name + ".xlsx", sheet_name="sheet1", index=False)
        full_path = excel_path + "/" + time_now_to_string + name + ".xlsx"
        return full_path
        pass
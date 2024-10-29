from io import BytesIO
import datetime
import pandas as pd

# 结点的运行方式
RUN_TYPE = ["timing", "trigger"]

def get_file(path: str) -> BytesIO:
    # 将文件转换为字节流
    df = pd.read_excel(path,sheet_name=None,header=0,index_col=0)
    excel_keys = list(df.keys())
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='openpyxl')
    for k in range(len(excel_keys)):
        df = pd.read_excel(path,sheet_name=excel_keys[k],header=0,index_col=0)
        df.to_excel(writer, sheet_name=excel_keys[k])
    writer.close()
    return output

def change_character_code(input_cell):
    # 处理考勤系统的中文乱码
    return str(input_cell).encode('iso8859-1').decode('gbk')

def year_month_days_num(year: int, month: int)-> int:
    # 获取某年某月有多少天
    return month_days_mum(year_days_num(year), month)

def year_days_num(year: int)-> int:
    # 获取某年一共多少天
    # 这一年第一天和这一年最后一天
    startDay = str(year)+'-01-01'
    endDay = str(year)+'-12-31'
    # 天数
    year_days_mum = (datetime.datetime.strptime(endDay, "%Y-%m-%d") - datetime.datetime.strptime(startDay, "%Y-%m-%d")).days +1
    return year_days_mum

def month_days_mum(year_days: int, num: int) -> int:
    # 获取某年的某月一共多少天
    if num in (1, 3, 5, 7, 8, 10, 12):
        month_days = 31
    elif num == 2:
        if year_days==366:  # 为闰年
            month_days=29
        else:
            month_days = 28
    else:
        month_days = 30
    return month_days
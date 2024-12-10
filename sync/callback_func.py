import pandas as pd


def read_file(file_path: str) -> str:
    '''根据路径读取文件'''
    with open(file_path, mode='r', encoding='utf-8') as file:
        return file.read()
    
    
def trans_table_to_json(__data: pd.DataFrame) -> dict:
    '''将表格转换成字典'''
    return __data.to_dict(orient='records')
    
def compose(f, g):
    '''拼接函数'''
    def h(x):
        return f(g(x))
    return h
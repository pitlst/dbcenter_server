import pandas as pd

def read_file(file_path: str) -> str:
    '''根据路径读取文件'''
    with open(file_path, mode='r', encoding='utf-8') as file:
        return file.read()
    
def trans_table_to_json(__data: pd.DataFrame) -> dict:
    '''将表格转换成字典'''
    __data.replace({pd.NaT: None}, inplace=True)
    return __data.to_dict(orient='records')

def trans_json_to_table(__data: dict) -> pd.DataFrame:
    '''将字典转换成表格'''
    return pd.json_normalize(__data)
    
def compose(f, g):
    '''拼接函数'''
    def h(x):
        return f(g(x))
    return h
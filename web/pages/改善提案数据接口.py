import streamlit as st
from general import get_table

@st.cache_data(ttl=86400)
def get_data() -> list:
    data = []
    data.append(get_table("dm_ameliorate"))
    return data

st.title("改善提案数据接口")
with st.spinner('请稍等......正在读取数据并缓存......'):
    data = get_data()


st.download_button('改善提案数据接口-包括全员型改善和自主型改善', data = data[0], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
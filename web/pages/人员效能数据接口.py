import streamlit as st
from general import mysql_client

@st.cache_data
def get_data() -> list:
    data = []
    return data

st.title("人员效能数据下载")
with st.spinner('请稍等......正在读取数据并缓存......'):
    data = get_data()


st.download_button('人员效能导出-考勤部分', data = data[0], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-工时部分-班组', data = data[1], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-工时部分-车间', data = data[2], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-标杆产品用工量-月度', data = data[3], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-标杆产品用工量-季度', data = data[4], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-标杆产品用工量-年度', data = data[5], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
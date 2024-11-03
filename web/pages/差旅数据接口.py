import streamlit as st
from general import mysql_client

@st.cache_data
def get_data() -> list:
    data = []
    return data

st.title("差旅数据下载--未开发完成")
# with st.spinner('请稍等......正在读取数据并缓存......'):
#     data = get_data()

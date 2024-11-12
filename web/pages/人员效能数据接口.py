import streamlit as st
from general import get_table

@st.cache_data(ttl=86400)
def get_data() -> list:
    data = []
    data.append(get_table("dm_kq_time"))
    data.append(get_table("dm_work_time_group"))
    data.append(get_table("dm_work_time_department"))
    data.append(get_table("dm_product_labor_month"))
    data.append(get_table("dm_product_labor_quarter"))
    data.append(get_table("dm_product_labor_year"))
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
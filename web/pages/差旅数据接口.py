import streamlit as st
from general.commont import get_file

@st.cache_data
def get_data() -> list:
    data = []
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_kq_time.xlsx"
    data.append(get_file(path).getvalue())
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_work_time_group.xlsx"
    data.append(get_file(path).getvalue())
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_work_time_department.xlsx"
    data.append(get_file(path).getvalue())
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_product_labor_month.xlsx"
    data.append(get_file(path).getvalue())
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_product_labor_quarter.xlsx"
    data.append(get_file(path).getvalue())
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_product_labor_year.xlsx"
    data.append(get_file(path).getvalue())
    return data

st.title("差旅数据下载--未开发完成")
# with st.spinner('请稍等......正在读取数据并缓存......'):
#     data = get_data()

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

st.title("人员效能数据下载")
with st.spinner('请稍等......正在读取数据并缓存......'):
    data = get_data()


st.download_button('人员效能导出-考勤部分', data = data[0], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-工时部分-班组', data = data[1], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-工时部分-车间', data = data[2], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-标杆产品用工量-月度', data = data[3], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-标杆产品用工量-季度', data = data[4], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('人员效能导出-标杆产品用工量-年度', data = data[5], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
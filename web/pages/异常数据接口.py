import streamlit as st
from general.commont import get_file

@st.cache_data
def get_data() -> list:
    data = []
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_abnormal.xlsx"
    data.append(get_file(path).getvalue())
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_abnormal_zl_department.xlsx"
    data.append(get_file(path).getvalue())
    path = r"C:\\Users\\Administrator\\Documents\\dbcenter_dag_process\\dag\\file\\file\\dm_abnormal_zl_project.xlsx"
    data.append(get_file(path).getvalue())
    return data

st.title("异常数据下载")
with st.spinner('请稍等......正在读取数据并缓存......'):
    data = get_data()

st.download_button('📥异常明细下载', data = data[0], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.markdown("暂时仅支持质量技术部月报的导出，以下数据都限定为**质量技术部相关**")
st.download_button('质量技术部异常月报下载--部门', data = data[1], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")
st.download_button('质量技术部异常月报下载--项目', data = data[2], file_name = 'temp.xlsx', mime="application/vnd.ms-excel")


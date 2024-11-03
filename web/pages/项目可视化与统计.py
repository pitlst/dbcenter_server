import streamlit as st
import os
# 定义代码所在的目录
base_path = os.path.join(os.path.dirname(__file__), "../../")


# 在指定目录下统计所有的py文件，以列表形式返回
def collect_files(dir):
    filelist = []
    for parent, _, filenames in os.walk(dir):
        for filename in filenames:
            if filename.endswith('.py') or \
               filename.endswith('.sql') or \
               filename.endswith('.cpp') or \
               filename.endswith('.hpp') or \
               filename.endswith('.sh') or \
               filename.endswith('.ps1'):
                # 将文件名和目录名拼成绝对路径，添加到列表里
                filelist.append(os.path.join(parent, filename))
    return filelist


# 计算单个文件内的代码行数
def calc_linenum(file):
    with open(file, encoding="utf8", mode='r') as fp:
        content_list = fp.readlines()
        code_num = 0  # 当前文件代码行数计数变量
        blank_num = 0  # 当前文件空行数计数变量
        annotate_num = 0  # 当前文件注释行数计数变量
        for content in content_list:
            content = content.strip()
            # 统计空行
            if content == '':
                blank_num += 1
            # 统计注释行
            elif content.startswith('#') or content.startswith("--"):
                annotate_num += 1
            # 统计代码行
            else:
                code_num += 1
    # 返回代码行数，空行数，注释行数
    return code_num, blank_num, annotate_num


@st.cache_data(ttl=33600)
def main():
    files = collect_files(base_path)
    total_code_num = 0   # 统计文件代码行数计数变量
    total_blank_num = 0   # 统计文件空行数计数变量
    total_annotate_num = 0  # 统计文件注释行数计数变量
    for f in files:
        code_num, blank_num, annotate_num = calc_linenum(f)
        total_code_num += code_num
        total_blank_num += blank_num
        total_annotate_num += annotate_num

    return total_code_num, total_blank_num, total_annotate_num


st.title("项目可视化与统计")
total_code_num, total_blank_num, total_annotate_num = main()
st.markdown("代码总行数为： " + str(total_code_num))
st.markdown("空行总行数为： " + str(total_blank_num))
st.markdown("注释行总行数为： " + str(total_annotate_num))

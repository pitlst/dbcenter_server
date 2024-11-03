


def change_character_code(input_cell):
    # 处理考勤系统的中文乱码
    return str(input_cell).encode('iso8859-1').decode('gbk')
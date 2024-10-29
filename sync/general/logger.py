import os
import datetime
import logging

# 节点日志
def node_logger(
                name: str, 
                path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "log"), 
                level_console:int = logging.DEBUG, 
                level_file:int = logging.DEBUG
                ) -> logging.Logger:
    
    file_path = os.path.join(path, name, datetime.datetime.now().strftime('%Y-%m-%d--%H-%M-%S') + ".log")
    dirs1 = os.path.dirname(file_path)
    if not os.path.exists(dirs1):
        os.makedirs(dirs1)

    LOG = logging.getLogger(name)
    LOG.setLevel(logging.DEBUG)

    # 设置log日志的标准输出打印
    console = logging.StreamHandler()
    console.setLevel(level_console)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s ' + name + ' %(message)s')
    console.setFormatter(formatter)
    LOG.addHandler(console)
    # 设置log日志文件
    fileio = logging.FileHandler(file_path, mode='a+', encoding="utf-8", delay=True)
    fileio.setLevel(level_file)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s ' + name + ' %(message)s')
    fileio.setFormatter(formatter)
    LOG.addHandler(fileio)
    return LOG
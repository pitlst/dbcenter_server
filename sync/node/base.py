import os
import abc
import toml
import time
import datetime
from general.config import SYNC_CONFIG
from general.connect import database_connect
from general.logger import node_logger


# 当前服务的所有节点数的计数
__total_node_num__ = 0

class node_base(abc.ABC):
    '''
    节点的基类
    对于该类处理，其输入输出均需要在数据库中备份
    所以这类节点的输入输出都应当是数据库和文件这类io,不提供变量模式的传递
    '''
    def __init__(self, name:str, temp_db: database_connect, type_name:str) -> None:
        global __total_node_num__
        # 每调用一次就加一
        __total_node_num__ += 1
        
        self.name = name
        self.type = type_name
        # 数据库连接
        self.temp_db = temp_db
        # 日志
        self.LOG = node_logger(self.name)
        # 下一次运行的时间
        self.last_time = datetime.datetime.now()
        # 获取配置
        self.MAX_NODE_FLOW_CAP = SYNC_CONFIG["max_node_flow_cap"]

    def run(self) -> str:
        self.LOG.info("开始计算")
        t = time.perf_counter()
        data_size = 0
        try:
            self.connect()
            data_size = self.read()
            self.write()
        except Exception as me:
            self.LOG.error(me)
        t = time.perf_counter() - t
        self.LOG.info("计算耗时为" + str(t) + "s")
        self.release()
        self.LOG.info("已释放资源")
        self.update(data_size)
        self.LOG.info("已更新状态")
        self.LOG.info("------------" + self.name + "计算结束------------")
        return self.name

    @abc.abstractmethod
    def connect(self):
        ...
    
    @abc.abstractmethod
    def read(self):
        ...

    @abc.abstractmethod
    def write(self):
        ...
        
    @abc.abstractmethod
    def release(self):
        ...
    
    def update(self, size: int) -> None:
        '''节点更新自己的状态'''
        s = size / (self.MAX_NODE_FLOW_CAP / __total_node_num__)
        self.last_time = datetime.datetime.now() + datetime.timedelta(seconds=s)
        
        

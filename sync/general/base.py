import abc
import time
from general.connect import database_connect  # 用于类型标注
from general.logger import node_logger


class node_base(abc.ABC):
    '''
    节点的基类
    对于该类处理，其输入输出均需要在数据库中备份
    所以这类节点的输入输出都应当是数据库和文件这类io,不提供变量模式的传递
    '''
    def __init__(self, name:str, temp_db: database_connect, type_name:str) -> None:
        self.name = name
        self.type = type_name
        # 数据库连接
        self.temp_db = temp_db
        # 日志
        self.LOG = node_logger(self.name)
        # -------这些是计算调度需要的标志位--------
        # 是否需要运行的标志位
        self.need_run = True
        # 数据大小，用于计算同步间隔时间
        self.data_size = 0

    def run(self) -> str:
        self.LOG.info("开始计算")
        t = time.perf_counter()
        try:
            self.connect()
            self.data_size = self.read()
            self.write()
        except Exception as me:
            self.LOG.error(me)
        t = time.perf_counter() - t
        self.LOG.info("计算耗时为" + str(t) + "s")
        self.release()
        self.LOG.info("已释放资源")
        self.LOG.info("------------" + self.name + "计算结束------------")
        self.need_run = False
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
        

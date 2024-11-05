import abc
import time
from general.connect import db_engine
from general.logger import node_logger


class dag_node_base(abc.ABC):
    '''
    dag调度的基类
    '''
    __all_node = set()
    def __init__(self, name:str, next_name:list[str], type_name:str = "node") -> None:
        # 因为需要注册相关数据的原因，需要在子类构造完成后再进行父类构造
        super().__init__()
        if name in dag_node_base.__all_node:
            raise Exception("节点名称重复，重复的名称为：" + name)
        else:
            dag_node_base.__all_node.add(name)
        self.name = name
        self.next_name = next_name
        self.type = type_name
        # 调度的标志位
        self.is_run = False
        
    @abc.abstractmethod
    def run(self) -> None:
        self.is_run = True
        return self.name

class db_process_base(dag_node_base):
    '''
    数据库数据处理的基类
    对于该类处理，其输入输出均需要在数据库中备份
    所以这类节点的输入输出都应当是数据库和文件这类io,不提供变量模式的传递
    '''
    def __init__(self, name:str, next_name:list[str], type_name:str = "process") -> None:
        super().__init__(name, next_name)
        self.temp_db = db_engine
        self.type = type_name
        self.LOG = node_logger(self.name)

    def run(self) -> str:
        self.LOG.info("------------开始" + self.name + "计算------------")
        t = time.perf_counter()
        try:
            self.connect()
            self.read()
            self.process()
            self.write()
        except Exception as me:
            self.LOG.error(me)
        t = time.perf_counter() - t
        self.LOG.info("计算耗时为" + str(t) + "s")
        self.release()
        self.LOG.info("已释放资源")
        self.LOG.info("------------" + self.name + "计算结束------------")
        return super().run()

    @abc.abstractmethod
    def connect(self) -> None:
        ...
    
    @abc.abstractmethod
    def read(self) -> None:
        ...

    @abc.abstractmethod
    def write(self) -> None:
        ...
        
    @abc.abstractmethod
    def process(self) -> None:
        ...
        
    @abc.abstractmethod
    def release(self) -> None:
        ...
        


    

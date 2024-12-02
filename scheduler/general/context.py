import datetime
from enum import Enum
from general.connect import MONGO_CLIENT

class node_status(Enum):
    '''节点状态'''
    not_run = 0
    send_start = 1
    is_running = 2
    runned = 3

class mongo_mq:
    '''消息队列抽象'''
    def __init__(self) -> None:
        database = MONGO_CLIENT["public"]        
        time_series_options = {
            "timeField": "timestamp",
            "metaField": "message"
        }
        if "message_queue_send" not in database.list_collection_names():
            self.collection_send = database.create_collection("message_queue_send", timeseries=time_series_options)
        else:
            self.collection_send = database["message_queue_send"]
        if "message_queue_recv" not in database.list_collection_names():
            self.collection_recv = database.create_collection("message_queue_recv", timeseries=time_series_options)
        else:
            self.collection_recv = database["message_queue_recv"]
            
    def start_node(self, node_name: str | list[str]) -> None:
        '''在消息队列中通知开始执行对应节点'''
        assert isinstance(node_name, str) or isinstance(node_name, list), "输入的变量类型不正确"
        if isinstance(node_name, str):
            self.collection_send.insert_one({
                "timestamp": datetime.datetime.now(),
                "message": {
                    "节点": node_name,
                    "消息": "开始执行"
                }
            })
        elif isinstance(node_name, list):
            for node_name_ in node_name:
                self.collection_send.insert_one({
                    "timestamp": datetime.datetime.now(),
                    "message": {
                        "节点": node_name_,
                        "消息": "开始执行"
                    }
                })

    def get_node_status(self, node_name: str) -> node_status:
        '''获取对应节点状态'''
        ...
        
    def get_node_status(self) -> dict[str, node_status]:
        '''获取当前所有节点的状态'''
        ...
        
class context:
    '''节点状态的上下文抽象'''
    def __init__(self) -> None:
        pass
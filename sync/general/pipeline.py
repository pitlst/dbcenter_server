import datetime
from general import MONGO_CLIENT

class pipeline:
    def __init__(self):
        database = MONGO_CLIENT["public"]
        coll_list = database.list_collection_names()
        assert "mq_send" in coll_list and "mq_recv" in coll_list, "管道未初始化"
        self.coll_send = database["mq_send"]
        self.coll_recv = database["mq_recv"]
        
    def send(self, node_name: str) -> None:
        self.coll_recv.insert_one({
            "timestamp": datetime.datetime.now(),
            "node_name": node_name,
            "is_process": False
        })
        
    def recv(self, node_name: str) -> list:
        temp_doc_list = self.coll_send.find({"node_name": node_name, "is_process":False}).to_list()
        for tdoc in temp_doc_list:
            self.coll_send.update_one({"_id":tdoc["_id"]}, {"$set": {"is_process":True}})
        return temp_doc_list
    
    def recv_history(self, node_name: str) -> list:
        '''获取对应节点的历史消息'''
        return self.coll_send.find({"node_name": node_name, "is_process":True}).to_list()
    
# 全局单例
PPL = pipeline()
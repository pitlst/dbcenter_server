import os
import json
import datetime
import numpy as np

from general.connect import MONGO_CLIENT
from general.logger import LOG

# 节点两次同步变化的系数
NODE_SYNC_K = 204800
# 节点两次同步的最小间隔，该参数用在计算节点同步数据量的使用
NODE_SYNC_MIN = 600
# 节点两次同步的最大间隔，该参数用在计算节点同步数据量的使用
NODE_SYNC_MAX = 3600


if __name__ == "__main__":
    # ------------------------------------------获取节点配置------------------------------------------
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source", "config", "tasks.json"), "r", encoding="utf-8") as file:
        node_json = json.load(file)
    name_set = set()
    for ch in node_json:
        if ch["name"] not in name_set:
            name_set.add(ch["name"])
        else:
            raise ValueError("节点名称重复" + ch["name"])
    # ------------------------------------------获取节点依赖------------------------------------------
    NODE_DEPEND: dict[str, dict[str, list[str]]] = {}
    for ch in node_json:
        if ch["name"] in NODE_DEPEND.keys():
            NODE_DEPEND[ch["name"]]["front"] = ch["next_name"]
        else:
            NODE_DEPEND[ch["name"]] = {"front": ch["next_name"], "back": []}
        for ch_ in ch["next_name"]:
            if ch_ in NODE_DEPEND.keys():
                NODE_DEPEND[ch_]["back"].append(ch["name"])  
            else:
                NODE_DEPEND[ch_] = {"front": [], "back": []}
                NODE_DEPEND[ch_]["back"].append(ch["name"])  
    # ------------------------------------------获取节点子图------------------------------------------
    def get_deps(node_name: str, orientations: str) -> set[str]:
        '''获取当前节点的所有前后依赖'''
        temp_set = set()
        for ch in NODE_DEPEND[node_name][orientations]:
            temp_set.add(ch)
            temp_set = temp_set.union(get_deps(ch, orientations))
        return temp_set
    NODE_DAG: list[set[str]] = []
    for ch in NODE_DEPEND:
        temp = set()
        temp.add(ch)
        NODE_DAG.append(get_deps(ch, "front") | get_deps(ch, "back") | temp)

    # ------------------------------------------获取或者更新调度上下文------------------------------------------
    not_same = False
    temp_coll = MONGO_CLIENT["public"]["context"].find({},{ "_id": 0, "node": 1}).to_list()
    if len(temp_coll) == 0:
        not_same = True
    else:
        for ch in temp_coll:
            ch = ch["node"]
            for ch_ in ch:
                if ch_["name"] not in NODE_DEPEND:
                    not_same = True
                    break
                if list(ch_["front"]) != list(NODE_DEPEND[ch_["name"]]["front"]):
                    not_same = True
                    break
                if list(ch_["back"]) != list(NODE_DEPEND[ch_["name"]]["back"]):
                    not_same = True
                    break
            if not_same:
                break
    if not_same:
        MONGO_CLIENT["public"]["context"].drop()
        all_doc = []
        for ch in NODE_DAG:
            temp_dict = {}
            temp_dict["last_time"] = datetime.datetime.now()
            temp_dict["is_running"] = False
            temp_dict["data_size"] = 0
            temp_dict["node"] = []
            for ch_ in ch:
                temp_dict_ = {}
                temp_dict_["name"] = ch_
                temp_dict_["front"] = NODE_DEPEND[ch_]["front"]
                temp_dict_["back"] = NODE_DEPEND[ch_]["back"]
                temp_dict_["status"] = "不需要运行"    # 可能有三种状态，不需要运行，需要运行，已经运行完成
                temp_dict["node"].append(temp_dict_)
            all_doc.append(temp_dict)
        MONGO_CLIENT["public"]["context"].insert_many(all_doc)
        
    
    # ------------------------------------------开始调度------------------------------------------
    while True:
        temp_coll = []
        temp_coll_ = MONGO_CLIENT["public"]["context"].find().to_list()
        change_label = False
        for index, dag_node in enumerate(temp_coll_):
            if dag_node["is_running"]:
                all_node_is_runned = True
                for node_ in dag_node["node"]:
                    if node_["status"] == "需要运行":
                        all_node_is_runned = False
                    elif node_["status"] == "不需要运行":
                        all_node_is_runned = False
                        # 寻找同子图下的节点
                        all_deps_is_runned = True
                        for node__ in node_["front"]:
                            for node____ in dag_node["node"]:
                                if node__ == node____["name"] and node____["status"] != "已经运行完成":
                                    all_deps_is_runned = False
                                    break
                            if not all_deps_is_runned:
                                break
                        if all_deps_is_runned:
                            change_label = True
                            LOG.info("节点" + str(node_["name"]) + "开始触发运行")
                            node_["status"] = "需要运行"
                if all_node_is_runned:
                    change_label = True
                    LOG.debug("子图" + str(index) + "已经全部运行完成")
                    temp_second = np.arctan(np.sqrt(dag_node["data_size"]) / NODE_SYNC_K) / np.pi * 2 * (NODE_SYNC_MAX - NODE_SYNC_MIN) + NODE_SYNC_MAX
                    dag_node["last_time"] = datetime.datetime.now() + datetime.timedelta(seconds=temp_second)
                    dag_node["is_running"] = False
                    dag_node["data_size"] = 0
                    for node_ in dag_node["node"]:
                        node_["status"] = "不需要运行"
            else:
                if dag_node["last_time"] <= datetime.datetime.now():
                    change_label = True
                    LOG.debug("子图" + str(index) + "开始触发运行")
                    dag_node["is_running"] = True
                    for node_ in dag_node["node"]:
                        if len(node_["front"]) == 0:
                            LOG.info("节点" + str(node_["name"]) + "开始触发运行")
                            node_["status"] = "需要运行"
            temp_coll.append(dag_node)
        if change_label:
            # 替换整个集合
            for ch in temp_coll:
                MONGO_CLIENT["public"]["context"].replace_one({"_id": ch["_id"]}, ch)
                            
        
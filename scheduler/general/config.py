import os
import json

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
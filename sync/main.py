import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from general import TASKS_CONFIG, LOG, node, PPL
from callback_func import compose, read_file, trans_table_to_json, trans_json_to_table

if __name__ == "__main__":
    all_node: dict[str, node] = {}
    for task in TASKS_CONFIG:
        # 前处理注入
        if task["type"] in ["sql_to_table", "sql_to_excel", "sql_to_csv", "sql_to_nosql", "sql_to_json", "json_to_nosql"]:
            preprocess_func = read_file
        else:
            preprocess_func = None
        # 后处理注入
        if task["type"] in ["sql_to_nosql", "table_to_nosql", "excel_to_nosql", "csv_to_nosql"]:
            postprocess_func = trans_table_to_json
        elif task["type"] in ["nosql_to_table", "nosql_to_excel", "nosql_to_csv"]:
            postprocess_func = trans_json_to_table
        else:
            postprocess_func = None
        
        '''在这里添加自定义的依赖注入'''    
            
        # 过滤处理节点
        if task["type"] != "process":
            all_node[task["name"]] = node(task, preprocess_func=preprocess_func, postprocess_func=postprocess_func)
        
    with ThreadPoolExecutor() as tpool:
        while True:
            run_list = []
            for task in all_node:
                is_request = PPL.recv(task)
                if len(is_request) != 0:
                    LOG.debug(task + "触发运行")
                    run_list.append(tpool.submit(all_node[task].run))
            if len(run_list) != 0:
                for ch in as_completed(run_list):
                    PPL.send(ch.result())
            else:
                LOG.info("无任务在运行，等待5秒")
                time.sleep(5)
            
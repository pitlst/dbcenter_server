import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from general import TASKS_CONFIG, LOG, node, PPL
from callback_func import compose, read_file, trans_table_to_json

if __name__ == "__main__":
    all_node: dict[str, node] = {}
    for task in TASKS_CONFIG:
        # 特殊的依赖注入
        # 普遍的依赖注入
        # 前处理注入
        if task["type"] in ["sql_to_table", "sql_to_excel", "sql_to_csv", "sql_to_nosql", "sql_to_json", "json_to_nosql"]:
            preprocess_func = read_file
        else:
            preprocess_func = None
        # 后处理注入
        if task["type"] in ["sql_to_nosql", "table_to_nosql", "excel_to_nosql", "csv_to_nosql"]:
            postprocess_func = trans_table_to_json
        else:
            postprocess_func = None
        # 过滤自定义的处理
        if task["type"] != "process":
            all_node[task["name"]] = node(task, preprocess_func=preprocess_func, postprocess_func=postprocess_func)
        
    with ThreadPoolExecutor() as tpool:
        while True:
            run_list = []
            for task in all_node:
                LOG.info("检查" + task + "是否该运行")
                is_request = PPL.recv(task)
                if len(is_request) != 0:
                    run_list.append(tpool.submit(all_node[task].run))
                else:
                    LOG.debug(task + "不需要运行")
            if len(run_list) != 0:
                for ch in as_completed(run_list):
                    PPL.send(ch.result())
            else:
                LOG.info("无任务在运行，等待5秒")
                time.sleep(5)
            
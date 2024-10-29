import concurrent.futures    # 用于类型标注
from concurrent.futures import ThreadPoolExecutor, as_completed
from general.base import db_process_base # 用于类型标注

def run(node_list: list[db_process_base]) -> None:
    # 启动线程池
    node_map, node_next_map = process_name(node_list)
    with ThreadPoolExecutor() as tpool:
        total_tasks = []
        final_node = set()
        while True:
            # 获取当前可以运行的节点
            run_node = get_node(final_node, node_next_map)
            # 如果获取不到可以执行的节点即退出
            if len(run_node) == 0:
                break
            # 将节点添加到线程池
            for ch in run_node:
                total_tasks.append(tpool.submit(node_map[ch].run))
            # 等待节点运行完成
            for future in as_completed(total_tasks):
                final_node.add(future.result())

def process_name(node_list: list[db_process_base]) -> tuple[dict[str, db_process_base], dict[str, list[str]]]:
    new_node_map = {}
    new_node_next_map = {}
    for ch in node_list:
        new_node_map[ch.name] = ch
        new_node_next_map[ch.name] = ch.next_name
    return new_node_map, new_node_next_map

def get_node(final_node: set, node_next_map: dict) -> list[str]:
    # 获取当前可以执行的节点
    run_node = []
    for ch in node_next_map:
        if ch in final_node:
            continue
        else:
            label = True
            for ch0 in node_next_map[ch]:
                if ch0 not in final_node:
                    label = False
                    break
            if label:
                run_node.append(ch)
    return run_node

def update_final_node(final_node:set, total_tasks: dict[str, concurrent.futures.Future]):
    # 更新当前那已经执行完的节点
    for ch in total_tasks:
        if ch in final_node:
            continue
        elif total_tasks[ch].done():
            final_node.add(ch)
    return final_node   
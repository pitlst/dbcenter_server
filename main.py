import os
import time
import subprocess

if __name__ == "__main__":
    results = []
    PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))
    # 启动web服务
    
    # 启动调度器
    scheduler_path = os.path.join(PROJECT_PATH, "scheduler")
    results.append(subprocess.Popen(['python', os.path.join(scheduler_path, "main.py")], 
                                    cwd=scheduler_path, 
                                    close_fds=os.name != 'nt',  # 在Windows上不需要关闭文件描述符
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                                    )
                )
    time.sleep(1)
    # 启动同步器
    sync_path = os.path.join(PROJECT_PATH, "sync")
    results.append(subprocess.Popen(['python', os.path.join(sync_path, "main.py")], 
                                    cwd=sync_path, 
                                    close_fds=os.name != 'nt',  # 在Windows上不需要关闭文件描述符
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                                    )
                )
    time.sleep(1)
    # 启动各个定制的处理器
    # 访客系统
    process_path = os.path.join(PROJECT_PATH, "process", "build", "Release")
    results.append(subprocess.Popen(["chcp", "65001", "&&", os.path.join(process_path, "dbcenter_server_visitor.exe")], 
                                    cwd=process_path, 
                                    close_fds=os.name != 'nt',  # 在Windows上不需要关闭文件描述符
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                                    )
                )
    # 设计变更统计
    results.append(subprocess.Popen(["chcp", "65001", "&&", os.path.join(process_path, "dbcenter_server_bc_join.exe")], 
                                    cwd=process_path, 
                                    close_fds=os.name != 'nt',  # 在Windows上不需要关闭文件描述符
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                                    )
                )
    
    
    
    

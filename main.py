import os
import time
import subprocess


if __name__ == "__main__":
    results = []
    PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))
    # 先启动调度器
    scheduler_path = os.path.join(PROJECT_PATH, "scheduler")
    results.append(subprocess.Popen(['python', os.path.join(scheduler_path, "main.py")], 
                                    cwd=scheduler_path, 
                                    close_fds=os.name != 'nt',  # 在Windows上不需要关闭文件描述符
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                                    )
                )
    time.sleep(1)
    # 再启动同步器
    sync_path = os.path.join(PROJECT_PATH, "sync")
    results.append(subprocess.Popen(['python', os.path.join(sync_path, "main.py")], 
                                    cwd=sync_path, 
                                    close_fds=os.name != 'nt',  # 在Windows上不需要关闭文件描述符
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                                    )
                )
    time.sleep(1)
    # 最后启动各个定制的处理器
    # 访客系统
    visitor_path = os.path.join(PROJECT_PATH, "process", "build", "Release")
    results.append(subprocess.Popen([os.path.join(visitor_path, "dbcenter_server_visitor.exe")], 
                                    cwd=visitor_path, 
                                    close_fds=os.name != 'nt',  # 在Windows上不需要关闭文件描述符
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
                                    )
                )
    
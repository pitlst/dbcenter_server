import time
import datetime
import schedule
from general import LOG, PPL, context

def main():
    LOG.debug("定时开始执行")
    temp_context = context()
    while not temp_context.is_final():
        temp_tasks = temp_context.get_ready_to_run()
        if len(temp_tasks) != 0:
            LOG.debug("触发以下节点开始执行:" + ",".join(temp_tasks))
        else:
            LOG.debug("未触发节点执行")
        for ch in temp_tasks:
            PPL.send(ch)
        LOG.debug("检查节点状态")
        temp_tasks = temp_context.get_not_finish()
        for ch in temp_tasks:
            temp_list = PPL.recv(ch)
            if len(temp_list) != 0:
                temp_context.update(ch)
                LOG.debug("节点" + ch + "运行完成")
        # 别轮询太快给数据库太大压力，等一秒
        time.sleep(1)
    LOG.debug("所有节点运行完成")
    
def clean_mq():
    # 每天清除30天前的消息
    LOG.info("触发清除消息队列")
    requeat_day = datetime.datetime.now() - datetime.timedelta(days=30)
    deleted_count = PPL.clean_send_history(requeat_day)
    LOG.info("清除了" + str(deleted_count) + "条发送记录")
    deleted_count = PPL.clean_recv_history(requeat_day)
    LOG.info("清除了" + str(deleted_count) + "条接收记录")
    
if __name__ == "__main__":
    schedule.every().hours.do(main)
    schedule.every().days.do(clean_mq)
    
    LOG.debug("调度器开始执行......首次启动立即运行所有作业")
    schedule.run_all()
    LOG.debug("首次运行完成，开始定时调度")
    while True:
        schedule.run_pending()
        time.sleep(1)
        
                            
        
#include <iostream>
#include "business_connection/join.hpp"
#include "business_connection/process.hpp"
#include "message/format.hpp"
#include "message/sum.hpp"
#include "person/person.hpp"
#include "visitor/visitor.hpp"

int main()
{
    std::thread logger_server(LOGGER.get_run_func());
    LOGGER.debug("root", "并发日志服务已启动");

    tbb::task_group tg;
    std::vector<std::function<void()>> task_list;

    dbs::task_bc_join bc_join;
    task_list.emplace_back(bc_join.get_run_func());
    dbs::task_bc_process bc_process;
    task_list.emplace_back(bc_process.get_run_func());
    dbs::task_msg_format msg_format;
    task_list.emplace_back(msg_format.get_run_func());
    dbs::task_msg_sum msg_sum;
    task_list.emplace_back(msg_sum.get_run_func());
    dbs::task_person person;
    task_list.emplace_back(person.get_run_func());
    dbs::task_visitor visitor;
    task_list.emplace_back(visitor.get_run_func());

    LOGGER.debug("root", "初始化完成，开始运行任务");
    for (const auto &ch : task_list)
    {
        tg.run(ch);
    }

    tg.wait();
    logger_server.join();
    return 0;
}
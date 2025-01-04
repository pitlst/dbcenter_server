#include <iostream>

#include <tbb/scalable_allocator.h> // TBB 的可扩展内存分配器
#include <tbb/tbb_allocator.h>      // TBB 的默认内存分配器
// 替换全局默认的内存分配器为 TBB 的 scalable_allocator
#define __TBB_ALLOCATOR_CONSTRUCTOR tbb::scalable_allocator

#include "business_connection/join.hpp"
#include "business_connection/process.hpp"
#include "message/format.hpp"
#include "message/sum.hpp"
#include "person/person.hpp"
#include "visitor/visitor.hpp"

using namespace std::chrono_literals;

int main()
{
    // 设置最大线程数为系统核心数的二倍
    unsigned int num_cores = std::thread::hardware_concurrency();
    tbb::global_control global_limit(tbb::global_control::max_allowed_parallelism, int(num_cores * 1.5));


    tbb::task_group continue_tg;
    continue_tg.run(LOGGER.get_run_func());
    continue_tg.run(PIPELINE.get_run_func());
    LOGGER.debug("root", "并发日志服务已启动");

    std::vector<std::function<void()>> task_list;
    // 业联系统数据拼接
    dbs::task_bc_join_class_group bc_join_class_group;
    task_list.emplace_back(bc_join_class_group.get_run_func());
    dbs::task_bc_join_technological_process bc_join_technological_process;
    task_list.emplace_back(bc_join_technological_process.get_run_func());
    dbs::task_bc_join_business_connection bc_join_business_connectio;
    task_list.emplace_back(bc_join_business_connectio.get_run_func());
    dbs::task_bc_join_design_change bc_join_design_change;
    task_list.emplace_back(bc_join_design_change.get_run_func());
    dbs::task_bc_join_shop_execution bc_join_shop_execution;
    task_list.emplace_back(bc_join_shop_execution.get_run_func());
    dbs::task_bc_join_design_change_execution bc_join_design_change_execution;
    task_list.emplace_back(bc_join_design_change_execution.get_run_func());
    // 业联系统数据处理
    // dbs::task_bc_process bc_process;
    // task_list.emplace_back(bc_process.get_run_func());
    // 薪酬数据处理
    dbs::task_msg_format msg_format;
    task_list.emplace_back(msg_format.get_run_func());
    dbs::task_msg_sum msg_sum;
    task_list.emplace_back(msg_sum.get_run_func());
    // 人员数据处理
    // dbs::task_person person;
    // task_list.emplace_back(person.get_run_func());
    // 外网访客系统数据处理
    dbs::task_visitor visitor;
    task_list.emplace_back(visitor.get_run_func());

    tbb::task_group yield_tg;
    LOGGER.debug("root", "初始化完成，开始运行任务");
    while (true)
    {
        for (const auto &ch : task_list)
        {
            yield_tg.run(ch);
        }
        std::this_thread::sleep_for(1000ms);
    }
    yield_tg.wait();
    continue_tg.wait();
    return 0;
}
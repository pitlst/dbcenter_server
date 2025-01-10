#include <iostream>
#include <chrono>

#include <tbb/scalable_allocator.h> // TBB 的可扩展内存分配器
#include <tbb/tbb_allocator.h>      // TBB 的默认内存分配器
// 替换全局默认的内存分配器为 TBB 的 scalable_allocator
#define __TBB_ALLOCATOR_CONSTRUCTOR tbb::scalable_allocator

#include "visitor/visitor.hpp"
#include "business_connection/increment.hpp"
#include "business_connection/join.hpp"
#include "message/format.hpp"
#include "message/sum.hpp"

using namespace std::chrono_literals;

int main()
{
    tbb::task_group continue_tg;
    continue_tg.run(LOGGER.get_run_func());
    continue_tg.run(PIPELINE.get_run_func());
    LOGGER.debug("root", "并发日志服务已启动");

    while (true)
    {
        std::vector<std::function<void()>> task_list;
        // 外网访客系统数据处理
        dbs::task_visitor visitor;
        task_list.emplace_back(visitor.get_run_func());

        // 苍穹业联数据处理
        dbs::increment_class_group increment_class_group;
        task_list.emplace_back(increment_class_group.get_run_func());
        dbs::increment_business_connection increment_business_connection;
        task_list.emplace_back(increment_business_connection.get_run_func());
        dbs::increment_design_change increment_design_change;
        task_list.emplace_back(increment_design_change.get_run_func());
        dbs::increment_technological_process increment_technological_process;
        task_list.emplace_back(increment_technological_process.get_run_func());
        dbs::increment_shop_execution increment_shop_execution;
        task_list.emplace_back(increment_shop_execution.get_run_func());
        dbs::increment_design_change_execution increment_design_change_execution;
        task_list.emplace_back(increment_design_change_execution.get_run_func());
        dbs::increment_business_connection_close increment_business_connection_close;
        task_list.emplace_back(increment_business_connection_close.get_run_func());

        dbs::task_bc_join_class_group task_bc_join_class_group;
        task_list.emplace_back(task_bc_join_class_group.get_run_func());
        dbs::task_bc_join_technological_process task_bc_join_technological_process;
        task_list.emplace_back(task_bc_join_technological_process.get_run_func());
        dbs::task_bc_join_business_connection task_bc_join_business_connection;
        task_list.emplace_back(task_bc_join_business_connection.get_run_func());
        dbs::task_bc_join_design_change task_bc_join_design_change;
        task_list.emplace_back(task_bc_join_design_change.get_run_func());
        dbs::task_bc_join_shop_execution task_bc_join_shop_execution;
        task_list.emplace_back(task_bc_join_shop_execution.get_run_func());
        dbs::task_bc_join_design_change_execution task_bc_join_design_change_execution;
        task_list.emplace_back(task_bc_join_design_change_execution.get_run_func());
        dbs::task_bc_join_business_connection_close task_bc_join_business_connection_close;
        task_list.emplace_back(task_bc_join_business_connection_close.get_run_func());

        // 短信信息处理
        dbs::task_msg_increment task_msg_increment;
        task_list.emplace_back(task_msg_increment.get_run_func());
        dbs::task_msg_format task_msg_format;
        task_list.emplace_back(task_msg_format.get_run_func());

        dbs::task_msg_sum task_msg_sum;
        task_list.emplace_back(task_msg_sum.get_run_func());

        tbb::task_group yield_tg;
        LOGGER.debug("root", "初始化完成，开始运行任务");
        auto start_time = std::chrono::steady_clock::now();
        while (true)
        {
            auto current_time = std::chrono::steady_clock::now(); 
            auto elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time).count(); 
            if (elapsed_time >= 86400)  // 一整天 
            {
                LOGGER.debug("root", "定时已到，开始重新初始化对应类");
                break;
            }
            for (const auto &ch : task_list)
            {
                yield_tg.run(ch);
            }
            std::this_thread::sleep_for(1000ms);
        }
        LOGGER.debug("root", "等待现有类运行完成......");
        yield_tg.wait();
    }
    continue_tg.wait();
    return 0;
}
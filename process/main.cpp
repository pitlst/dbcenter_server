#include <iostream>

#include <tbb/scalable_allocator.h> // TBB 的可扩展内存分配器
#include <tbb/tbb_allocator.h>      // TBB 的默认内存分配器
// 替换全局默认的内存分配器为 TBB 的 scalable_allocator
#define __TBB_ALLOCATOR_CONSTRUCTOR tbb::scalable_allocator


#include "visitor/visitor.hpp"

using namespace std::chrono_literals;

int main()
{
    tbb::task_group continue_tg;
    continue_tg.run(LOGGER.get_run_func());
    continue_tg.run(PIPELINE.get_run_func());
    LOGGER.debug("root", "并发日志服务已启动");

    std::vector<std::function<void()>> task_list;
    // 外网访客系统数据处理
    dbs::task_visitor visitor;
    task_list.emplace_back(visitor.get_run_func());
    // 苍穹业联数据处理

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
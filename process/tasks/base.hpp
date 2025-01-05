#ifndef DBS_TASK_BASE_INCLUDE
#define DBS_TASK_BASE_INCLUDE

#include <iostream>
#include <thread>
#include <memory>
#include <atomic>  
#include <chrono>

#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"
#include "bsoncxx/json.hpp"

#include "oneapi/tbb.h"
#include "oneapi/tbb/tbbmalloc_proxy.h"

#include "mongo.hpp"
#include "logger.hpp"
#include "pipeline.hpp"

namespace dbs
{
    class task_base
    {
    public:
        task_base(const std::string & node_name);
        ~task_base() = default;
        
        // 获取任务执行函数
        std::function<void()> get_run_func();
        // 主逻辑
        virtual void main_logic() = 0;

    protected:
        // 任务名称
        std::string node_name;
        // 是否正在运行
        std::atomic<bool> is_running = false;
    };
} 

#endif
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

#include "nlohmann/json.hpp"

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
        // 获取集合
        mongocxx::collection get_coll(const std::string &db_name, const std::string &coll_name);
        // 获取集合的值
        tbb::concurrent_vector<nlohmann::json> get_coll_data(const std::string &db_name, const std::string &coll_name);

        // 任务名称
        std::string node_name;
        // 是否正在运行
        std::atomic<bool> is_running = false;
        // 对应线程的初始化
        mongocxx::pool::entry m_client = MONGO.init_client();
    };
} 

#endif
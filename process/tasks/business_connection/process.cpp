#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>

#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"
#include "bsoncxx/json.hpp"

#include "json.hpp"

#include "tbb/tbb.h"

#include "mongo.hpp"
#include "logger.hpp"
#include "pipeline.hpp"

#define MY_NAME "业联系统数据处理"

void main_logic()
{
    try
    {
        auto client = MONGO.init_client();
        // ----------从数据库读取数据----------
        LOGGER.info(MY_NAME, "读取数据");
        auto dwd_bc_design_change = MONGO.get_coll_data(client, "dwd", "业联-设计变更");
        auto dwd_bc_technological_process = MONGO.get_coll_data(client, "dwd", "业联-工艺流程");
        auto dwd_bc_design_change_execution = MONGO.get_coll_data(client, "dwd", "业联-设计变更执行");
        auto dwd_bc_close = MONGO.get_coll_data(client, "ods", "bc_business_connection_close");

        
    }
    catch (const std::exception &e)
    {
        LOGGER.error(MY_NAME, e.what());
    }
}


int main()
{
    using namespace std::chrono_literals;
    auto temp_pipe = dbs::pipeline(MY_NAME);
    std::thread logger_server(LOGGER.get_run_func());
    LOGGER.debug(MY_NAME, "并发日志服务已启动");
    tbb::task_group tg;
    while (true)
    {
        if(temp_pipe.recv())
        {
            LOGGER.debug(MY_NAME, "接到触发信号，开始执行");
            auto before = std::chrono::steady_clock::now();
            main_logic();
            temp_pipe.send();
            auto after = std::chrono::steady_clock::now();
            double duration_second = std::chrono::duration<double, std::milli>(after - before).count() / 1000;
            LOGGER.debug(MY_NAME, "执行完成，共计耗时" + std::to_string(duration_second) + "秒");
        }
        else
        {
        LOGGER.debug(MY_NAME, "未接到信号，等待5秒");
        std::this_thread::sleep_for(5000ms);
        }
    }
    logger_server.join();
    return 0;
}

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

#define MY_NAME "人员基础数据统计"

void main_logic()
{
    try
    {
        // ----------从数据库读取数据----------
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
    while (true)
    {
        if(temp_pipe.recv())
        {
            LOGGER.debug(MY_NAME, "接到触发信号，开始执行");
            main_logic();
            temp_pipe.send();
        }
        LOGGER.debug(MY_NAME, "未接到信号，等待5秒");
        std::this_thread::sleep_for(5000ms);
    }
    return 0;
}

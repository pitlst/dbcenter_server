#include "base.hpp"

using namespace dbs;

task_base::task_base(const std::string &node_name) : node_name(node_name)
{
}

std::function<void()> task_base::get_run_func()
{
    return [&]()
    {
        if (PIPELINE.recv(this->node_name))
        {
            LOGGER.debug(this->node_name, "接到触发信号，开始执行");
            auto before = std::chrono::steady_clock::now();
            try
            {
                this->main_logic();
            }
            catch (const std::exception &e)
            {
                LOGGER.error(this->node_name, e.what());
            }
            PIPELINE.send(this->node_name);
            auto after = std::chrono::steady_clock::now();
            double duration_second = std::chrono::duration<double, std::milli>(after - before).count() / 1000;
            LOGGER.debug(this->node_name, "执行完成，共计耗时" + std::to_string(duration_second) + "秒");
        }
        else
        {
            LOGGER.debug(this->node_name, "未接到信号，等待下次触发");
        }
    };
}
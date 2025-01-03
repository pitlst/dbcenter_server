#include "base.hpp"

using namespace dbs;

task_base::task_base(const std::string & node_name): node_name(node_name)
{
    this->m_pipe = std::make_unique<pipeline>(node_name);
}

std::function<void()> task_base::get_run_func()
{
    using namespace std::chrono_literals;
    return [&]()
    {
        while (true)
        {
            if(this->m_pipe->recv())
            {
                LOGGER.debug(this->node_name, "接到触发信号，开始执行");
                auto before = std::chrono::steady_clock::now();
                try
                {
                    this->main_logic();
                }
                catch(const std::exception& e)
                {
                    LOGGER.error(this->node_name, e.what());
                }
                this->m_pipe->send();
                auto after = std::chrono::steady_clock::now();
                double duration_second = std::chrono::duration<double, std::milli>(after - before).count() / 1000;
                LOGGER.debug(this->node_name, "执行完成，共计耗时" + std::to_string(duration_second) + "秒");
            }
            else
            {
                LOGGER.debug(this->node_name, "未接到信号，等待5秒");
                std::this_thread::sleep_for(5000ms);
            }
        }
    };
}
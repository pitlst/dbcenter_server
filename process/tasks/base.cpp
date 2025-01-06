#include "base.hpp"

using namespace dbs;

task_base::task_base(const std::string &node_name) : node_name(node_name)
{
}

std::function<void()> task_base::get_run_func()
{
    return [&]()
    {
        if (this->is_running)
        {
            LOGGER.info(this->node_name, "任务正在运行......");
        }
        else
        {
            if (PIPELINE.recv(this->node_name))
            {
                this->is_running = true;
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
                this->is_running = false;
            }
        }
    };
}

mongocxx::collection task_base::get_coll(const std::string &db_name, const std::string &coll_name)
{
    return this->m_client[db_name][coll_name];
}

tbb::concurrent_vector<nlohmann::json> task_base::get_coll_data(const std::string &db_name, const std::string &coll_name)
{
    auto results_cursor = this->m_client[db_name][coll_name].find({});
    tbb::concurrent_vector<nlohmann::json> results;
    for (auto &&ch : results_cursor)
    {
        results.emplace_back(nlohmann::json::parse(bsoncxx::to_json(ch)));
    }
    return results;
}

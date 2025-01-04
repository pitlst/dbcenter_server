#include "message/sum.hpp"

using namespace dbs;

void task_msg_sum::main_logic()
{
    LOGGER.info(this->node_name, "读取数据");
    auto client = MONGO.init_client();
    // ----------从数据库读取数据----------
    auto dwd_results = MONGO.get_coll_data(client, "dwd", "薪酬信息");
    // 薪酬数据组织
    // 0是工号，1是姓名，2是年月, 实领金额在队列中
    using tbb_pay = std::tuple<std::string, std::string, std::string, tbb::concurrent_queue<double>, double>;
    tbb::concurrent_vector<tbb_pay> employee_compensation;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json input_json = dwd_results[index];
            bool not_exist = true;
            for (auto &ch : employee_compensation)
            {
                auto temp_id = std::get<0>(ch);
                auto temp_year_month = std::get<2>(ch);
                if (temp_id == input_json["工号"].get<std::string>() && temp_year_month == input_json["年月"].get<std::string>())
                {
                    std::get<3>(ch).push(input_json["实领金额"].get<double>());
                    not_exist = false;
                    break;
                }
            }
            if (not_exist)
            {
                tbb::concurrent_queue<double> temp_queue;
                temp_queue.push(input_json["实领金额"].get<double>());
                tbb_pay temp = std::make_tuple(input_json["工号"].get<std::string>(), input_json["姓名"].get<std::string>(), input_json["年月"].get<std::string>(), temp_queue, 0.);
                employee_compensation.emplace_back(temp);
            }
        }
    };
    auto data_sum = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            auto &temp = employee_compensation[index];
            double final_sum = 0.;
            while (!std::get<3>(temp).empty())
            {
                double temp_double;
                if (std::get<3>(temp).try_pop(temp_double))
                {
                    final_sum += temp_double;
                }
            }
            std::get<4>(temp) = final_sum;
        }
    };
    tbb::concurrent_vector<bsoncxx::document::value> employee_compensation_res;
    auto data_trans = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            auto ch = employee_compensation[index];
            nlohmann::json results_json;
            results_json["工号"] = std::get<0>(ch);
            results_json["姓名"] = std::get<1>(ch);
            results_json["年月"] = std::get<2>(ch);
            results_json["实领金额"] = std::get<4>(ch);
            employee_compensation_res.emplace_back(bsoncxx::from_json(results_json.dump()));
        }
    };
    // 利用tbb加速
    if (dwd_results.empty())
    {
        LOGGER.info(this->node_name, "来源数据为空，不处理数据");
    }
    else
    {
        LOGGER.info(this->node_name, "并行处理数据-获取唯一的人与年月");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, dwd_results.size()), data_process);
        LOGGER.info(this->node_name, "并行处理数据-计算月工资之和");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, employee_compensation.size()), data_sum);
        LOGGER.info(this->node_name, "并行处理数据-转换格式");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, employee_compensation.size()), data_trans);
    }

    if (employee_compensation_res.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
    }
    else
    {
        LOGGER.info(this->node_name, "写入处理数据");
        auto m_coll = MONGO.get_coll(client, "dm", "employee_compensation");
        m_coll.drop();
        m_coll.insert_many(employee_compensation_res);
    }
}

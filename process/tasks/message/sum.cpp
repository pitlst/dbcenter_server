#include <variant>

#include "message/sum.hpp"

using namespace dbs;

void task_msg_sum::main_logic()
{
    LOGGER.info(this->node_name, "读取数据");
    auto client = MONGO.init_client();
    // ----------从数据库读取数据----------
    auto dwd_results = MONGO.get_coll_data(client, "dwd", "薪酬信息");
    // 薪酬数据组织
    using pay_data = std::variant<std::monostate, double, std::string, tbb::concurrent_queue<double>>;
    tbb::concurrent_set<tbb::concurrent_vector<std::string>> employee_compensation_set;
    tbb::concurrent_vector<tbb::concurrent_map<std::string, pay_data>> employee_compensation;
    auto data_polymaker = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json input_json = dwd_results[index];
            tbb::concurrent_vector<std::string> temp;
            temp.emplace_back(input_json["工号"].get<std::string>());
            temp.emplace_back(input_json["姓名"].get<std::string>());
            temp.emplace_back(input_json["年月"].get<std::string>());
            employee_compensation_set.emplace(temp);
        }
    };
    auto data_trans_vector = [&](const tbb::concurrent_vector<std::string> & value)
    {
        tbb::concurrent_map<std::string, pay_data> temp;
        temp["工号"] = value[0];
        temp["姓名"] = value[1];
        temp["年月"] = value[2];
        temp["实领金额"] = 0.0;
        temp["实领金额队列"] = tbb::concurrent_queue<double>();
        employee_compensation.emplace_back(temp);
    };
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json input_json = dwd_results[index];
            for (auto &ch : employee_compensation)
            {
                auto temp_id = std::get<std::string>(ch["工号"]);
                auto temp_year_month = std::get<std::string>(ch["年月"]);
                auto input_id = input_json["工号"].get<std::string>();
                auto input_year_month = input_json["年月"].get<std::string>();
                if (temp_id == input_id && temp_year_month == input_year_month)
                {
                    std::get<tbb::concurrent_queue<double>>(ch["实领金额队列"]).push(input_json["实领金额"].get<double>());
                    break;
                }
            }
        }
    };
    auto data_sum = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            auto &ch = employee_compensation[index];
            double sum = 0;
            double temp = 0;
            while (std::get<tbb::concurrent_queue<double>>(ch["实领金额队列"]).try_pop(temp))
            {
                sum += temp;
            }
            ch["实领金额"] = sum;
        }
    };
    tbb::concurrent_vector<bsoncxx::document::value> employee_compensation_res;
    auto data_trans_bson = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            auto ch = employee_compensation[index];
            nlohmann::json results_json;
            results_json["工号"] = std::get<std::string>(ch["工号"]);
            results_json["姓名"] = std::get<std::string>(ch["姓名"]);
            results_json["年月"] = std::get<std::string>(ch["年月"]);
            results_json["实领金额"] = std::get<double>(ch["实领金额"]);
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
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, dwd_results.size()), data_polymaker);
        tbb::parallel_for_each(employee_compensation_set.begin(), employee_compensation_set.end(), data_trans_vector);
        LOGGER.info(this->node_name, "并行处理数据-计算月工资之和");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, dwd_results.size()), data_process);
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, employee_compensation.size()), data_sum);
        LOGGER.info(this->node_name, "并行处理数据-转换格式");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, employee_compensation.size()), data_trans_bson);
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

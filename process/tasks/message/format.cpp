#include "message/format.hpp"

using namespace dbs;

void task_msg_format::main_logic()
{
    LOGGER.info(this->node_name, "读取数据");
    auto client = MONGO.init_client();
    // ----------从数据库读取数据----------
    auto ods_results = MONGO.get_coll_data(client, "ods", "short_message");
    auto dwd_results = MONGO.get_coll_data(client, "dwd", "薪酬信息");
    // ----------检查数据是否更新----------
    tbb::concurrent_vector<nlohmann::json> old_source_id;
    for (const auto &ch : dwd_results)
    {
        old_source_id.emplace_back(ch["source_id"]);
    }
    tbb::concurrent_vector<nlohmann::json> form_results;
    // 检查数据更新
    auto data_check = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            auto ch = ods_results[index];
            auto find_it = std::find(old_source_id.begin(), old_source_id.end(), ch["id"]);
            if (find_it == old_source_id.end())
            {
                form_results.emplace_back(ch);
            }
        }
    };
    // 薪酬数据组织
    tbb::concurrent_vector<bsoncxx::document::value> employee_compensation;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json input_json = form_results[index];
            auto msg_ = dbs::remove_newline(input_json["短信详情"].get<std::string>());
            if (msg_.find("薪酬信息") != std::string::npos)
            {
                // 提取数据
                nlohmann::json results_json;
                results_json["source_id"] = input_json["id"];
                std::vector<std::string> msg_vector_3 = dbs::split_string(msg_, "   ");
                if (msg_vector_3.size() < 2)
                {
                    LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                    continue;
                }
                results_json["年月"] = dbs::remove_substring(msg_vector_3[0], "月薪酬信息");
                std::vector<std::string> msg_vector_2 = dbs::split_string(msg_vector_3[1], "  ");
                if (msg_vector_2.size() < 3)
                {
                    LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                    continue;
                }
                results_json["工号"] = dbs::remove_substring(msg_vector_2[0], "员工编号:");
                std::vector<std::string> msg_vector_1 = dbs::split_string(msg_vector_2[1], " ");
                if (msg_vector_1.size() < 2)
                {
                    LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                    continue;
                }
                std::vector<std::string> msg_vector_name = dbs::split_string(msg_vector_1[0], "：");
                if (msg_vector_name.size() < 2)
                {
                    LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                    continue;
                }
                results_json["姓名"] = msg_vector_name[1];

                std::vector<std::string> msg_vector_money = dbs::split_string(msg_vector_1[1], ":");
                if (msg_vector_money.size() < 2)
                {
                    LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                    continue;
                }
                results_json["实领金额"] = std::stod(msg_vector_money[1]);
                std::vector<std::string> msg_vector_1_ = dbs::split_string(msg_vector_2[2], " ");
                results_json["实领分录"] = nlohmann::json::object();
                for (const auto &ch : msg_vector_1_)
                {
                    auto temp = dbs::split_string(ch, ":");
                    if (temp.size() == 2)
                    {
                        results_json["实领分录"][temp[0]] = std::stod(temp[1]);
                    }
                }
                // 插入数据
                employee_compensation.emplace_back(bsoncxx::from_json(results_json.dump()));
            }
        }
    };
    // 利用tbb加速
    if (ods_results.empty())
    {
        LOGGER.info(this->node_name, "来源数据为空，不更新数据");
    }
    else
    {
        LOGGER.info(this->node_name, "并行检查更新");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, ods_results.size()), data_check);
    }

    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无短信更新");
    }
    else
    {
        LOGGER.info(this->node_name, "并行处理数据");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, form_results.size()), data_process);
    }

    if (employee_compensation.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
    }
    else
    {
        LOGGER.info(this->node_name, "写入处理数据");
        auto m_coll = MONGO.get_coll(client, "dwd", "薪酬信息");
        m_coll.insert_many(employee_compensation);
    }
}
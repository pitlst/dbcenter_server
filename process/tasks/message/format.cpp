#include "message/format.hpp"

using namespace dbs;

void task_msg_format::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取数据");
    auto ods_results = this->get_coll_data("ods", "short_message");
    auto dwd_results = this->get_coll_data("dwd", "薪酬信息");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    // 短信内容不会发生变更，所以可以根据id做增量更新
    tbb::concurrent_vector<std::string> old_source_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        old_source_id.emplace_back(value["source_id"]["$oid"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_results.begin(), dwd_results.end(), extract_id);
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto data_check = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(old_source_id.begin(), old_source_id.end(), value["_id"]["$oid"].get<std::string>());
        if (find_it == old_source_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_results.begin(), ods_results.end(), data_check);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    // ----------并行处理数据----------
    LOGGER.info(this->node_name, "并行处理数据");
    // 薪酬数据组织
    tbb::concurrent_vector<bsoncxx::document::value> employee_compensation;
    auto check_size = [this](size_t input_size, size_t request_size, const std::string &temp_str)
    {
        if (input_size < request_size)
        {
            LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + temp_str);
            return;
        }
    };
    auto data_process = [&](const nlohmann::json &input_json)
    {
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
                return;
            }
            results_json["年月"] = dbs::remove_substring(msg_vector_3[0], "月薪酬信息");
            std::vector<std::string> msg_vector_2 = dbs::split_string(msg_vector_3[1], "  ");
            if (msg_vector_2.size() < 3)
            {
                LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                return;
            }
            results_json["工号"] = dbs::remove_substring(msg_vector_2[0], "员工编号:");
            std::vector<std::string> msg_vector_1 = dbs::split_string(msg_vector_2[1], " ");
            if (msg_vector_1.size() < 2)
            {
                LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                return;
            }
            std::vector<std::string> msg_vector_name = dbs::split_string(msg_vector_1[0], "：");
            if (msg_vector_name.size() < 2)
            {
                LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                return;
            }
            results_json["姓名"] = msg_vector_name[1];

            std::vector<std::string> msg_vector_money = dbs::split_string(msg_vector_1[1], ":");
            if (msg_vector_money.size() < 2)
            {
                LOGGER.warn(this->node_name, "短信内容不符合规范，跳过：" + msg_);
                return;
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
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);

    // ----------写入数据库----------
    if (employee_compensation.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
    }
    else
    {
        LOGGER.info(this->node_name, "写入处理数据");
        auto m_coll = this->get_coll("dwd", "薪酬信息");
        m_coll.insert_many(employee_compensation);
    }
}
#include <fstream>

#include "sql_builder.hpp"
#include "message/format.hpp"

using namespace dbs;

void task_msg_increment::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取数据");
    auto ods_results = this->get_coll_data("ods", "increment_short_message");
    auto dwd_results = this->get_coll_data("dwd", "薪酬信息");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        bool is_change = true;
        for (const auto &ch : dwd_results)
        {
            if (value["id"] == ch["id"])
            {
                is_change = false;
                break;
            }
        }
        if (is_change)
        {
            request_id.emplace_back(value["id"].get<std::string>());
        }
    };
    tbb::parallel_for_each(ods_results.begin(), ods_results.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    std::string sql_str = dbs::read_file(PROJECT_PATH + std::string("../source/select/short_message/sync_template/short_message.sql"));
    dbs::sql_builder temp_sql;
    temp_sql.init(sql_str);
    dbs::sql_builder temp_sql_2;
    if (request_id.empty())
    {
        temp_sql_2.add("msg.fid IS NULL");
    }
    else
    {
        for (const auto &ch : request_id)
        {
            temp_sql_2.add("msg.fid = " + ch, "OR");
        }
    }
    temp_sql.add(std::vector<std::string>{"to_number( to_char( SYSDATE, 'yyyy' ) ) - to_number( to_char( msg.FSENTTIME, 'yyyy' ) ) < 2 ", temp_sql_2.build()}, std::vector<std::string>{"AND"});
    dbs::write_file(PROJECT_PATH + std::string("../source/select/short_message/temp/short_message.sql"), temp_sql.build());
}

void task_msg_format::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取数据");
    auto ods_results = this->get_coll_data("ods", "short_message");

    // ----------并行处理数据----------
    LOGGER.info(this->node_name, "并行处理数据");
    // 薪酬数据组织
    tbb::concurrent_vector<std::pair<bsoncxx::document::value, bsoncxx::document::value>> employee_compensation;
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
            results_json["id"] = input_json["id"];
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
            nlohmann::json m_filter;
            m_filter["id"] = results_json["id"];
            auto res_input = std::make_pair(bsoncxx::from_json(m_filter.dump()), bsoncxx::from_json(results_json.dump()));
            employee_compensation.emplace_back(res_input);
        }
    };
    tbb::parallel_for_each(ods_results.begin(), ods_results.end(), data_process);

    // ----------写入数据库----------
    if (employee_compensation.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }
    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->get_coll("dwd", "薪酬信息");
    // 指定参数为更新或插入文档
    mongocxx::options::replace opts{}; 
    opts.upsert(true);
    for (const auto & input_ch : employee_compensation)
    {
        m_coll.replace_one(input_ch.first.view(), input_ch.second.view(), opts);
    }
}
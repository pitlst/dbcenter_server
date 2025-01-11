#include <sstream>
#include <fstream>

#include "ameliorate.hpp"

using namespace dbs;

void increment_ameliorate::main_logic()
{
    LOGGER.info(this->node_name, "读取改善数据");
    auto ods_table = this->get_coll_data("ods", "increment_ameliorate");
    auto dwd_table = this->get_coll_data("dwd", "改善数据");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        bool is_change = true;
        for (const auto &ch : dwd_table)
        {
            if (value["提案编号"] == ch["提案编号"])
            {
                is_change = false;
                break;
            }
        }
        if (is_change)
        {
            request_id.emplace_back(value["提案编号"].get<std::string>());
        }
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    std::stringstream ss;
    ss << dbs::read_file(PROJECT_PATH + std::string("../source/select/ameliorate/sync_template/ameliorate.sql"));
    if (request_id.empty())
    {
        ss << " " << "f.fk_crrc_proposal_no IS NULL";
    }
    else
    {
        ss << "(";
        for (auto it = request_id.begin(); it != request_id.end(); it++)
        {
            if (it == request_id.end() - 1)
            {
                ss << " " << "f.fk_crrc_proposal_no = " + *it;
            }
            else
            {
                ss << " " << "f.fk_crrc_proposal_no = " + *it << " " << "OR";
            }
        }
        ss << ")";
    }
    dbs::write_file(PROJECT_PATH + std::string("../source/select/ameliorate/temp/ameliorate.sql"), ss.str());
};

void task_ameliorate::main_logic()
{
    LOGGER.info(this->node_name, "读取改善数据");
    auto ameliorate = this->get_coll_data("ods", "ameliorate");
    if (ameliorate.empty())
    {
        LOGGER.warn(this->node_name, "更新数据为空，不更新数据");
        return;
    }
    LOGGER.info(this->node_name, "并行处理数据");
    tbb::concurrent_vector<std::pair<bsoncxx::document::value, bsoncxx::document::value>> ameliorate_results;
    auto data_process = [&](nlohmann::json results_json)
    {
        results_json.erase("_id");
        nlohmann::json m_filter;
        m_filter["提案编号"] = results_json["提案编号"];
        auto res_input = std::make_pair(bsoncxx::from_json(m_filter.dump()), bsoncxx::from_json(results_json.dump()));
        ameliorate_results.emplace_back(res_input);
    };
    tbb::parallel_for_each(ameliorate.begin(), ameliorate.end(), data_process);
    if (ameliorate_results.empty())
    {
        LOGGER.warn(this->node_name, "结果数据为空，不更新数据");
        return;
    }
    LOGGER.info(this->node_name, "更新或写入处理数据");
    auto m_bulk = this->get_coll("dwd", "改善数据").create_bulk_write();
    for (const auto &input_ch : ameliorate_results)
    {
        m_bulk.append(mongocxx::model::replace_one{input_ch.first.view(),input_ch.second.view()}.upsert(true));
    }
    auto result = m_bulk.execute();
    LOGGER.info(this->node_name, "更新了" + std::to_string(result->upserted_count()) + "条数据，写入了" + std::to_string(result->inserted_count()) + "条数据");
};
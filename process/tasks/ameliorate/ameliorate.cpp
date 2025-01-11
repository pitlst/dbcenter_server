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
        for (auto it = request_id.begin(); it!= request_id.end(); it++)
        {
            if (it == request_id.end() - 1)
            {
                ss << " " << "f.fk_crrc_proposal_no = " + *it << " " << "OR";
            }
            else
            {
                ss << " " << "f.fk_crrc_proposal_no = " + *it;
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
    auto ameliorate_department_index = this->get_coll_data("ods", "ameliorate_department_index");
    auto ameliorate_quality_group_index = this->get_coll_data("ods", "ameliorate_quality_group_index");
    auto ameliorate_synthesis_group_index = this->get_coll_data("ods", "ameliorate_synthesis_group_index");
    auto ameliorate_item_group_index = this->get_coll_data("ods", "ameliorate_item_group_index");
    auto ameliorate_delivery_group_index = this->get_coll_data("ods", "ameliorate_delivery_group_index");
    auto ameliorate_assembly_group_index = this->get_coll_data("ods", "ameliorate_assembly_group_index");

    
};
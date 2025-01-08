#include <fstream>

#include "sql_builder.hpp"
#include "increment.hpp"

using namespace dbs;

void increment_class_group::main_logic()
{
    LOGGER.info(this->node_name, "读取班组数据");
    auto ods_bc_class_group = this->get_coll_data("ods", "increment_bc_class_group");
    auto dwd_bc_class_group = this->get_coll_data("dwd", "业联-班组基础数据");


    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> dwd_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        dwd_id.emplace_back(value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_bc_class_group.begin(), dwd_bc_class_group.end(), extract_id);
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(dwd_id.begin(), dwd_id.end(), value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
        if (find_it == dwd_id.end())
        {
            request_id.emplace_back(value["id"]["$oid"].get<std::string>());
        }
    };
    tbb::parallel_for_each(ods_bc_class_group.begin(), ods_bc_class_group.end(), comparison_id);


    LOGGER.info(this->node_name, "生成SQL");
    auto sql_make = [request_id](const std::string & file_name)
    {
        std::string sql_str = dbs::read_file(PROJECT_PATH + std::string("source/select/business_connection/sync_template/") + file_name + ".sql");
        dbs::sql_builder temp_sql;
        temp_sql.init(sql_str);
        for (const auto & ch : request_id)
        {
            temp_sql.add("bill.fid = " + ch , "OR");
        }
        sql_str = temp_sql.build();
        dbs::write_file(PROJECT_PATH + std::string("source/select/business_connection/temp/class_group.sql"), sql_str);
    };
    // 单据头查询
    sql_make("class_group");
    sql_make("class_group_entry");
}
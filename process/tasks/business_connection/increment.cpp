#include <fstream>

#include "sql_builder.hpp"
#include "increment.hpp"

using namespace dbs;

void task_bc_increment::sql_make(const std::string &file_name, const tbb::concurrent_vector<std::string> &request_id) const
{
    std::string sql_str = dbs::read_file(PROJECT_PATH + std::string("../source/select/business_connection/sync_template/") + file_name + ".sql");
    dbs::sql_builder temp_sql;
    temp_sql.init(sql_str);
    if (request_id.empty())
    {
        temp_sql.add("bill.fid IS NULL");
    }
    else
    {
        for (const auto &ch : request_id)
        {
            temp_sql.add("bill.fid = " + ch, "OR");
        }
    }
    sql_str = temp_sql.build();
    dbs::write_file(PROJECT_PATH + std::string("../source/select/business_connection/temp/") + file_name + ".sql", sql_str);
}

void increment_class_group::main_logic()
{
    LOGGER.info(this->node_name, "读取班组数据");
    auto ods_table = this->get_coll_data("ods", "increment_bc_class_group");
    auto dwd_table = this->get_coll_data("dwd", "业联-班组基础数据");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        for (const auto & ch : dwd_table)
        {
            if (value["id"] == ch["id"] && value["修改时间"] == ch["修改时间"])
            {
                break;
            }
        }
        request_id.emplace_back(std::to_string(value["id"].get<long long>()));
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    this->sql_make("class_group", request_id);
    this->sql_make("class_group_entry", request_id);
}

void increment_business_connection::main_logic()
{
    LOGGER.info(this->node_name, "读取业务联系书数据");
    auto ods_table = this->get_coll_data("ods", "increment_bc_business_connection");
    auto dwd_table = this->get_coll_data("dwd", "业联-业务联系书");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        for (const auto & ch : dwd_table)
        {
            if (value["id"] == ch["id"] && value["修改时间"] == ch["修改时间"] && value["审核时间"] == ch["审核时间"] && value["签发时间"] == ch["签发时间"])
            {
                break;
            }
        }
        request_id.emplace_back(std::to_string(value["id"].get<long long>()));
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    this->sql_make("business_connection", request_id);
    this->sql_make("business_connection_main_delivery_unit", request_id);
    this->sql_make("business_connection_copy_delivery_unit", request_id);
}

void increment_design_change::main_logic()
{
    LOGGER.info(this->node_name, "读取设计变更数据");
    auto ods_table = this->get_coll_data("ods", "increment_bc_design_change");
    auto dwd_table = this->get_coll_data("dwd", "业联-设计变更");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        for (const auto & ch : dwd_table)
        {
            if (value["id"] == ch["id"] && value["发放日期"] == ch["发放日期"] && value["修改时间"] == ch["修改时间"])
            {
                break;
            }
        }
        request_id.emplace_back(std::to_string(value["id"].get<long long>()));
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    this->sql_make("design_change", request_id);
    this->sql_make("design_change_entry", request_id);
}

void increment_technological_process::main_logic()
{
    LOGGER.info(this->node_name, "读取工艺流程数据");
    auto ods_table = this->get_coll_data("ods", "increment_bc_technological_process");
    auto dwd_table = this->get_coll_data("dwd", "业联-工艺流程");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        for (const auto & ch : dwd_table)
        {
            if (value["id"] == ch["id"] && value["修改时间"] == ch["修改时间"])
            {
                break;
            }
        }
        request_id.emplace_back(std::to_string(value["id"].get<long long>()));
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    this->sql_make("technological_process", request_id);
    this->sql_make("technological_process_flow", request_id);
    this->sql_make("technological_process_change", request_id);
}

void increment_shop_execution::main_logic()
{
    LOGGER.info(this->node_name, "读取车间执行单数据");
    auto ods_table = this->get_coll_data("ods", "increment_bc_shop_execution");
    auto dwd_table = this->get_coll_data("dwd", "业联-车间执行单");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        for (const auto & ch : dwd_table)
        {
            if (value["id"] == ch["id"] && value["批准日期"] == ch["批准日期"] && value["修改时间"] == ch["修改时间"])
            {
                break;
            }
        }
        request_id.emplace_back(std::to_string(value["id"].get<long long>()));
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    this->sql_make("shop_execution", request_id);
    this->sql_make("shop_execution_audit", request_id);
    this->sql_make("shop_execution_handle", request_id);
    this->sql_make("shop_execution_copy_delivery_unit", request_id);
    this->sql_make("shop_execution_main_delivery_unit", request_id);
    this->sql_make("shop_execution_reworked_material_unit", request_id);
    this->sql_make("shop_execution_reworked_material", request_id);
    this->sql_make("shop_execution_task_item_point_unit", request_id);
    this->sql_make("shop_execution_task_item_point", request_id);
    this->sql_make("shop_exeecution_material_preparation_technology_unit_class", request_id);
    this->sql_make("shop_exeecution_material_preparation_technology_unit_process", request_id);
    this->sql_make("shop_exeecution_material_preparation_technology", request_id);
}

void increment_design_change_execution::main_logic()
{
    LOGGER.info(this->node_name, "读取设计变更执行数据");
    auto ods_table = this->get_coll_data("ods", "increment_bc_design_change_execution");
    auto dwd_table = this->get_coll_data("dwd", "业联-设计变更执行");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        for (const auto & ch : dwd_table)
        {
            if (value["id"] == ch["id"] && value["批准日期"] == ch["批准日期"] && value["修改时间"] == ch["修改时间"])
            {
                break;
            }
        }
        request_id.emplace_back(std::to_string(value["id"].get<long long>()));
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    this->sql_make("design_change_execution", request_id);
    this->sql_make("design_change_execution_reworked_material", request_id);
    this->sql_make("design_change_execution_reworked_material_unit", request_id);
    this->sql_make("design_change_execution_material_preparation_technology", request_id);
    this->sql_make("design_change_execution_material_preparation_technology_unit", request_id);
    this->sql_make("design_change_execution_material_change", request_id);
    this->sql_make("design_change_execution_main_delivery_unit", request_id);
    this->sql_make("design_change_execution_handle", request_id);
    this->sql_make("design_change_execution_document_change", request_id);
    this->sql_make("design_change_execution_document_change_unit", request_id);
    this->sql_make("design_change_execution_copy_delivery_unit", request_id);
    this->sql_make("design_change_execution_change_content", request_id);
    this->sql_make("design_change_execution_audit", request_id);
}

void increment_business_connection_close::main_logic()
{
    LOGGER.info(this->node_name, "读取业联执行关闭数据");
    auto ods_table = this->get_coll_data("ods", "increment_bc_business_connection_close");
    auto dwd_table = this->get_coll_data("dwd", "业联-业联执行关闭");

    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> request_id;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        for (const auto & ch : dwd_table)
        {
            if (value["id"] == ch["id"] && value["任务状态"] == ch["任务状态"])
            {
                break;
            }
        }
        request_id.emplace_back(std::to_string(value["id"].get<long long>()));
    };
    tbb::parallel_for_each(ods_table.begin(), ods_table.end(), comparison_id);

    LOGGER.info(this->node_name, "生成SQL");
    this->sql_make("business_connection_close", request_id);
}

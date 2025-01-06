#include "business_connection/join.hpp"

using namespace dbs;

void task_bc_join_class_group::main_logic()
{
    LOGGER.info(this->node_name, "读取班组数据");
    // ----------从数据库读取数据----------
    auto ods_bc_class_group = this->get_coll_data("ods", "bc_class_group");
    auto ods_bc_class_group_entry = this->get_coll_data("ods", "bc_class_group_entry");
    auto dwd_bc_class_group = this->get_coll_data("dwd", "业联-班组基础数据");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> dwd_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        // 根据单据头id和修改时间做更新
        dwd_id.emplace_back(value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_bc_class_group.begin(), dwd_bc_class_group.end(), extract_id);
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(dwd_id.begin(), dwd_id.end(), value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
        if (find_it == dwd_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_bc_class_group.begin(), ods_bc_class_group.end(), comparison_id);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    // ----------并行处理数据----------
    LOGGER.info(this->node_name, "并行处理数据");
    tbb::concurrent_vector<bsoncxx::document::value> class_group_results;
    auto data_process = [&](nlohmann::json results_json)
    {
        results_json.erase("_id");
        results_json["分录"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_class_group_entry)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["分录"].emplace_back(ch_copy);
            }
        }
        class_group_results.emplace_back(bsoncxx::from_json(results_json.dump()));
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);

    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->get_coll("dwd", "业联-班组基础数据");
    if (!class_group_results.empty())
    {
        m_coll.insert_many(class_group_results);
    }
    else
    {
        LOGGER.warn(this->node_name, "结果数据为空，不更新数据");
    }
}

void task_bc_join_technological_process::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取工艺流程数据");
    auto ods_bc_technological_process = this->get_coll_data("ods", "bc_technological_process");
    auto ods_bc_technological_process_change = this->get_coll_data("ods", "bc_technological_process_change");
    auto ods_bc_technological_process_flow = this->get_coll_data("ods", "bc_technological_process_flow");
    auto dwd_bc_technological_process = this->get_coll_data("dwd", "业联-工艺流程");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> dwd_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        // 根据单据头id和修改时间做更新
        dwd_id.emplace_back(value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_bc_technological_process.begin(), dwd_bc_technological_process.end(), extract_id);
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(dwd_id.begin(), dwd_id.end(), value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
        if (find_it == dwd_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_bc_technological_process.begin(), ods_bc_technological_process.end(), comparison_id);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    // ----------并行处理数据----------
    LOGGER.info(this->node_name, "并行处理数据");
    tbb::concurrent_vector<bsoncxx::document::value> technological_process_results;
    auto data_process = [&](nlohmann::json results_json)
    {
        results_json.erase("_id");
        results_json["工艺变更分录"] = nlohmann::json::array();
        results_json["任务流程分录"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_technological_process_change)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["工艺变更分录"].emplace_back(ch_copy);
            }
        }
        for (const auto &ch : ods_bc_technological_process_flow)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["任务流程分录"].emplace_back(ch_copy);
            }
        }
        technological_process_results.emplace_back(bsoncxx::from_json(results_json.dump()));
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);
    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->get_coll("dwd", "业联-工艺流程");
    if (!technological_process_results.empty())
    {
        m_coll.insert_many(form_results);
    }
    else
    {
        LOGGER.warn(this->node_name, "结果数据为空，不更新数据");
    }
}

void task_bc_join_business_connection::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取业务联系数据");
    auto ods_bc_business_connection = this->get_coll_data("ods", "bc_business_connection");
    auto ods_bc_business_connection_main_delivery_unit = this->get_coll_data("ods", "bc_business_connection_main_delivery_unit");
    auto ods_bc_business_connection_copy_delivery_unit = this->get_coll_data("ods", "bc_business_connection_copy_delivery_unit");
    auto dwd_bc_business_connection = this->get_coll_data("dwd", "业联-业务联系书");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> dwd_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        // 根据单据头id和修改时间做更新
        dwd_id.emplace_back(value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_bc_business_connection.begin(), dwd_bc_business_connection.end(), extract_id);
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(dwd_id.begin(), dwd_id.end(), value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
        if (find_it == dwd_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_bc_business_connection.begin(), ods_bc_business_connection.end(), comparison_id);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    tbb::concurrent_vector<bsoncxx::document::value> business_connection_results;
    auto data_process = [&](nlohmann::json results_json)
    {
        results_json.erase("_id");
        results_json["主送单位"] = nlohmann::json::array();
        results_json["抄送单位"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_business_connection_main_delivery_unit)
        {
            if (ch["对应单据id"] == results_json["id"])
            {
                results_json["主送单位"].emplace_back(ch["对应基础资料id"]);
            }
        }
        for (const auto &ch : ods_bc_business_connection_copy_delivery_unit)
        {
            if (ch["对应单据id"] == results_json["id"])
            {
                results_json["抄送单位"].emplace_back(ch["对应基础资料id"]);
            }
        }
        form_results.emplace_back(bsoncxx::from_json(results_json.dump()));
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);
    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->get_coll("dwd", "业联-业务联系书");
    if (!business_connection_results.empty())
    {
        m_coll.insert_many(form_results);
    }
    else
    {
        LOGGER.warn(this->node_name, "结果数据为空，不更新数据");
    }
}

void task_bc_join_design_change::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取设计变更数据");
    auto ods_bc_design_change = this->get_coll_data("ods", "bc_design_change");
    auto ods_bc_design_change_entry = this->get_coll_data("ods", "bc_design_change_entry");
    auto dwd_bc_design_change = this->get_coll_data("ods", "业联-设计变更");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> dwd_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        // 根据单据头id和修改时间做更新
        dwd_id.emplace_back(value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_bc_design_change.begin(), dwd_bc_design_change.end(), extract_id);
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(dwd_id.begin(), dwd_id.end(), value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
        if (find_it == dwd_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_bc_design_change.begin(), ods_bc_design_change.end(), comparison_id);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    tbb::concurrent_vector<bsoncxx::document::value> design_change_results;
    auto data_process = [&](nlohmann::json results_json)
    {
        results_json.erase("_id");
        results_json["分录"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_entry)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["分录"].emplace_back(ch_copy);
            }
        }
        form_results.emplace_back(bsoncxx::from_json(results_json.dump()));
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);
    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->get_coll("dwd", "业联-设计变更");
    if (!design_change_results.empty())
    {
        m_coll.insert_many(form_results);
    }
    else
    {
        LOGGER.warn(this->node_name, "结果数据为空，不更新数据");
    }
}

void task_bc_join_shop_execution::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取车间执行数据");

    auto ods_bc_shop_execution = this->get_coll_data("ods", "bc_shop_execution");
    auto ods_bc_shop_execution_audit = this->get_coll_data("ods", "bc_shop_execution_audit");
    auto ods_bc_shop_execution_handle = this->get_coll_data("ods", "bc_shop_execution_handle");
    auto ods_bc_shop_execution_copy_delivery_unit = this->get_coll_data("ods", "bc_shop_execution_copy_delivery_unit");
    auto ods_bc_shop_execution_main_delivery_unit = this->get_coll_data("ods", "bc_shop_execution_main_delivery_unit");
    auto ods_bc_shop_execution_reworked_material = this->get_coll_data("ods", "bc_shop_execution_reworked_material");
    auto ods_bc_shop_execution_reworked_material_unit = this->get_coll_data("ods", "bc_shop_execution_reworked_material_unit");
    auto ods_bc_shop_execution_task_item_point = this->get_coll_data("ods", "bc_shop_execution_task_item_point");
    auto ods_bc_shop_execution_task_item_point_unit = this->get_coll_data("ods", "bc_shop_execution_task_item_point_unit");
    auto dwd_bc_shop_execution = this->get_coll_data("dwd", "业联-车间执行单");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> dwd_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        // 根据单据头id和修改时间做更新
        dwd_id.emplace_back(value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_bc_shop_execution.begin(), dwd_bc_shop_execution.end(), extract_id);
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(dwd_id.begin(), dwd_id.end(), value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
        if (find_it == dwd_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_bc_shop_execution.begin(), ods_bc_shop_execution.end(), comparison_id);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    // 城轨事业部的业联暂时不关注备料工艺
    tbb::concurrent_vector<bsoncxx::document::value> shop_execution_results;
    auto data_process = [&](nlohmann::json results_json)
    {
        results_json.erase("_id");
        results_json["审核人分录"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_shop_execution_audit)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["审核人分录"].emplace_back(ch_copy);
            }
        }
        results_json["经办人分录"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_shop_execution_handle)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["经办人分录"].emplace_back(ch_copy);
            }
        }
        results_json["主送单位"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_shop_execution_main_delivery_unit)
        {
            if (ch["对应单据id"] == results_json["id"])
            {
                results_json["主送单位"].emplace_back(ch["对应基础资料id"]);
            }
        }
        results_json["抄送单位"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_shop_execution_handle)
        {
            if (ch["对应单据id"] == results_json["id"])
            {
                results_json["抄送单位"].emplace_back(ch["对应基础资料id"]);
            }
        }
        results_json["任务项点"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_shop_execution_task_item_point)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy["执行班组"] = nlohmann::json::array();
                for (const auto &ch_ : ods_bc_shop_execution_task_item_point_unit)
                {
                    if (ch_["对应单据id"] == ch_copy["id"])
                    {
                        ch_copy["执行班组"].emplace_back(ch_["对应基础资料id"]);
                    }
                }
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["任务项点"].emplace_back(ch_copy);
            }
        }
        results_json["返工物料"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_shop_execution_reworked_material)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy["领料班组"] = nlohmann::json::array();
                for (const auto &ch_ : ods_bc_shop_execution_reworked_material_unit)
                {
                    if (ch_["对应单据id"] == ch_copy["id"])
                    {
                        ch_copy["领料班组"].emplace_back(ch_["对应基础资料id"]);
                    }
                }
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["返工物料"].emplace_back(ch_copy);
            }
        }
        form_results.emplace_back(bsoncxx::from_json(results_json.dump()));
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);
    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->get_coll("dwd", "业联-车间执行单");
    if (!shop_execution_results.empty())
    {
        m_coll.insert_many(form_results);
    }
    else
    {
        LOGGER.warn(this->node_name, "结果数据为空，不更新数据");
    }
}

void task_bc_join_design_change_execution::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "读取设计变更执行数据");
    auto ods_bc_design_change_execution = this->get_coll_data("ods", "bc_design_change_execution");
    auto ods_bc_design_change_execution_audit = this->get_coll_data("ods", "bc_design_change_execution_audit");
    auto ods_bc_design_change_execution_handle = this->get_coll_data("ods", "bc_design_change_execution_handle");
    auto ods_bc_design_change_execution_change_content = this->get_coll_data("ods", "bc_design_change_execution_change_content");
    auto ods_bc_design_change_execution_main_delivery_unit = this->get_coll_data("ods", "bc_design_change_execution_main_delivery_unit");
    auto ods_bc_design_change_execution_copy_delivery_unit = this->get_coll_data("ods", "bc_design_change_execution_copy_delivery_unit");
    auto ods_bc_design_change_execution_document_change = this->get_coll_data("ods", "bc_design_change_execution_document_change");
    auto ods_bc_design_change_execution_document_change_unit = this->get_coll_data("ods", "bc_design_change_execution_document_change_unit");
    auto ods_bc_design_change_execution_material_change = this->get_coll_data("ods", "bc_design_change_execution_material_change");
    auto ods_bc_design_change_execution_reworked_material = this->get_coll_data("ods", "bc_design_change_execution_reworked_material");
    auto ods_bc_design_change_execution_reworked_material_unit = this->get_coll_data("ods", "bc_design_change_execution_reworked_material_unit");
    auto dwd_bc_design_change_execution = this->get_coll_data("dwd", "业联-设计变更执行");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    tbb::concurrent_vector<std::string> dwd_id;
    auto extract_id = [&](const nlohmann::json &value)
    {
        // 根据单据头id和修改时间做更新
        dwd_id.emplace_back(value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
    };
    tbb::parallel_for_each(dwd_bc_design_change_execution.begin(), dwd_bc_design_change_execution.end(), extract_id);
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto comparison_id = [&](const nlohmann::json &value)
    {
        auto find_it = std::find(dwd_id.begin(), dwd_id.end(), value["id"]["$oid"].get<std::string>() + value["修改时间"]["$date"].get<std::string>());
        if (find_it == dwd_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_bc_design_change_execution.begin(), ods_bc_design_change_execution.end(), comparison_id);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    tbb::concurrent_vector<bsoncxx::document::value> design_change_execution_results;
    auto data_process = [&](nlohmann::json results_json)
    {
        results_json.erase("_id");
        results_json["审核人分录"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_audit)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["审核人分录"].emplace_back(ch_copy);
            }
        }
        results_json["经办人分录"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_handle)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["经办人分录"].emplace_back(ch_copy);
            }
        }
        results_json["主送单位"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_main_delivery_unit)
        {
            if (ch["对应单据id"] == results_json["id"])
            {
                results_json["主送单位"].emplace_back(ch["对应基础资料id"]);
            }
        }
        results_json["抄送单位"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_copy_delivery_unit)
        {
            if (ch["对应单据id"] == results_json["id"])
            {
                results_json["抄送单位"].emplace_back(ch["对应基础资料id"]);
            }
        }
        results_json["变更内容"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_change_content)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["变更内容"].emplace_back(ch_copy);
            }
        }
        results_json["返工工艺"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_reworked_material)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy["执行班组"] = nlohmann::json::array();
                for (const auto &ch_ : ods_bc_design_change_execution_reworked_material_unit)
                {
                    if (ch_["对应单据id"] == ch_copy["id"])
                    {
                        ch_copy["执行班组"].emplace_back(ch_["对应基础资料id"]);
                    }
                }
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["返工工艺"].emplace_back(ch_copy);
            }
        }
        results_json["物料变更"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_material_change)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["物料变更"].emplace_back(ch_copy);
            }
        }
        results_json["文件变更"] = nlohmann::json::array();
        for (const auto &ch : ods_bc_design_change_execution_document_change)
        {
            if (ch["id"] == results_json["id"])
            {
                nlohmann::json ch_copy = ch;
                ch_copy["发布班组"] = nlohmann::json::array();
                for (const auto &ch_ : ods_bc_design_change_execution_document_change_unit)
                {
                    if (ch_["对应单据id"] == ch_copy["id"])
                    {
                        ch_copy["执行班组"].emplace_back(ch_["对应基础资料id"]);
                    }
                }
                ch_copy.erase("_id");
                ch_copy.erase("id");
                results_json["文件变更"].emplace_back(ch_copy);
            }
        }
        form_results.emplace_back(bsoncxx::from_json(results_json.dump()));
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);
    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->get_coll("dwd", "业联-设计变更执行");
    if (!design_change_execution_results.empty())
    {
        m_coll.insert_many(form_results);
    }
    else
    {
        LOGGER.warn(this->node_name, "结果数据为空，不更新数据");
    }
}
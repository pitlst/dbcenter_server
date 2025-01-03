#include "business_connection/join.hpp"

using namespace dbs;

void task_bc_join_class_group::main_logic()
{
    auto client = MONGO.init_client();
    LOGGER.info(this->node_name, "读取班组数据");
    // ----------从数据库读取数据----------
    auto ods_bc_class_group = MONGO.get_coll_data(client, "ods", "bc_class_group");
    auto ods_bc_class_group_entry = MONGO.get_coll_data(client, "ods", "bc_class_group_entry");
    // 因为金蝶云苍穹的id在内容变更后不会更改，所以无法做增量计算，每一次都只能全量复写
    tbb::concurrent_vector<bsoncxx::document::value, tbb::scalable_allocator<bsoncxx::document::value>> form_results;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json results_json = ods_bc_class_group[index];
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
            form_results.emplace_back(bsoncxx::from_json(results_json.dump()));
        }
    };
    if (!ods_bc_class_group.empty())
    {
        LOGGER.info(this->node_name, "并行处理班组数据");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, ods_bc_class_group.size()), data_process);
        LOGGER.info(this->node_name, "写入班组数据");
        auto m_coll = MONGO.get_coll(client, "dwd", "业联-班组基础数据");
        if (!form_results.empty())
        {
            m_coll.drop();
            m_coll.insert_many(form_results);
        }
    }
    else
    {
        LOGGER.warn(this->node_name, "源数据为空，不更新数据");
    }
}

void task_bc_join_technological_process::main_logic()
{

    auto client = MONGO.init_client();
    LOGGER.info(this->node_name, "读取工艺流程数据");
    // ----------从数据库读取数据----------
    auto ods_bc_technological_process = MONGO.get_coll_data(client, "ods", "bc_technological_process");
    auto ods_bc_technological_process_change = MONGO.get_coll_data(client, "ods", "bc_technological_process_change");
    auto ods_bc_technological_process_flow = MONGO.get_coll_data(client, "ods", "bc_technological_process_flow");

    tbb::concurrent_vector<bsoncxx::document::value, tbb::scalable_allocator<bsoncxx::document::value>> form_results;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json results_json = ods_bc_technological_process[index];
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
            form_results.emplace_back(bsoncxx::from_json(results_json.dump()));
        }
    };
    if (!ods_bc_technological_process.empty())
    {
        LOGGER.info(this->node_name, "并行处理工艺流程数据");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, ods_bc_technological_process.size()), data_process);
        LOGGER.info(this->node_name, "写入工艺流程数据");
        auto m_coll = MONGO.get_coll(client, "dwd", "业联-工艺流程");
        if (!form_results.empty())
        {
            m_coll.drop();
            m_coll.insert_many(form_results);
        }
    }
    else
    {
        LOGGER.warn(this->node_name, "源数据为空，不更新数据");
    }
}

void task_bc_join_business_connection::main_logic()
{

    auto client = MONGO.init_client();
    LOGGER.info(this->node_name, "读取业务联系数据");
    // ----------从数据库读取数据----------
    auto ods_bc_business_connection = MONGO.get_coll_data(client, "ods", "bc_business_connection");
    auto ods_bc_business_connection_main_delivery_unit = MONGO.get_coll_data(client, "ods", "bc_business_connection_main_delivery_unit");
    auto ods_bc_business_connection_copy_delivery_unit = MONGO.get_coll_data(client, "ods", "bc_business_connection_copy_delivery_unit");

    tbb::concurrent_vector<bsoncxx::document::value, tbb::scalable_allocator<bsoncxx::document::value>> form_results;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json results_json = ods_bc_business_connection[index];
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
        }
    };
    if (!ods_bc_business_connection.empty())
    {
        LOGGER.info(this->node_name, "并行处理业务联系数据");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, ods_bc_business_connection.size()), data_process);
        LOGGER.info(this->node_name, "写入业务联系数据");
        auto m_coll = MONGO.get_coll(client, "dwd", "业联-业务联系书");
        if (!form_results.empty())
        {
            m_coll.drop();
            m_coll.insert_many(form_results);
        }
    }
    else
    {
        LOGGER.warn(this->node_name, "源数据为空，不更新数据");
    }
}

void task_bc_join_design_change::main_logic()
{

    auto client = MONGO.init_client();
    LOGGER.info(this->node_name, "读取设计变更数据");
    // ----------从数据库读取数据----------
    auto ods_bc_design_change = MONGO.get_coll_data(client, "ods", "bc_design_change");
    auto ods_bc_design_change_entry = MONGO.get_coll_data(client, "ods", "bc_design_change_entry");

    tbb::concurrent_vector<bsoncxx::document::value, tbb::scalable_allocator<bsoncxx::document::value>> form_results;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json results_json = ods_bc_design_change[index];
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
        }
    };
    if (!ods_bc_design_change.empty())
    {
        LOGGER.info(this->node_name, "并行处理设计变更数据");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, ods_bc_design_change.size()), data_process);
        LOGGER.info(this->node_name, "写入设计变更数据");
        auto m_coll = MONGO.get_coll(client, "dwd", "业联-设计变更");
        if (!form_results.empty())
        {
            m_coll.drop();
            m_coll.insert_many(form_results);
        }
    }
    else
    {
        LOGGER.warn(this->node_name, "源数据为空，不更新数据");
    }
}

void task_bc_join_shop_execution::main_logic()
{

    auto client = MONGO.init_client();
    LOGGER.info(this->node_name, "读取车间执行数据");
    // ----------从数据库读取数据----------
    auto ods_bc_shop_execution = MONGO.get_coll_data(client, "ods", "bc_shop_execution");
    auto ods_bc_shop_execution_audit = MONGO.get_coll_data(client, "ods", "bc_shop_execution_audit");
    auto ods_bc_shop_execution_handle = MONGO.get_coll_data(client, "ods", "bc_shop_execution_handle");
    auto ods_bc_shop_execution_copy_delivery_unit = MONGO.get_coll_data(client, "ods", "bc_shop_execution_copy_delivery_unit");
    auto ods_bc_shop_execution_main_delivery_unit = MONGO.get_coll_data(client, "ods", "bc_shop_execution_main_delivery_unit");
    auto ods_bc_shop_execution_reworked_material = MONGO.get_coll_data(client, "ods", "bc_shop_execution_reworked_material");
    auto ods_bc_shop_execution_reworked_material_unit = MONGO.get_coll_data(client, "ods", "bc_shop_execution_reworked_material_unit");
    auto ods_bc_shop_execution_task_item_point = MONGO.get_coll_data(client, "ods", "bc_shop_execution_task_item_point");
    auto ods_bc_shop_execution_task_item_point_unit = MONGO.get_coll_data(client, "ods", "bc_shop_execution_task_item_point_unit");
    // 城轨事业部的业联暂时不关注备料工艺

    tbb::concurrent_vector<bsoncxx::document::value, tbb::scalable_allocator<bsoncxx::document::value>> form_results;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json results_json = ods_bc_shop_execution[index];
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
        }
    };
    if (!ods_bc_shop_execution.empty())
    {
        LOGGER.info(this->node_name, "并行处理车间执行数据");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, ods_bc_shop_execution.size()), data_process);
        LOGGER.info(this->node_name, "写入车间执行数据");
        auto m_coll = MONGO.get_coll(client, "dwd", "业联-车间执行单");
        if (!form_results.empty())
        {
            m_coll.drop();
            m_coll.insert_many(form_results);
        }
    }
    else
    {
        LOGGER.warn(this->node_name, "源数据为空，不更新数据");
    }
}

void task_bc_join_design_change_execution::main_logic()
{

    auto client = MONGO.init_client();
    LOGGER.info(this->node_name, "读取设计变更执行数据");
    auto ods_bc_design_change_execution = MONGO.get_coll_data(client, "ods", "bc_design_change_execution");
    auto ods_bc_design_change_execution_audit = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_audit");
    auto ods_bc_design_change_execution_handle = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_handle");
    auto ods_bc_design_change_execution_change_content = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_change_content");
    auto ods_bc_design_change_execution_main_delivery_unit = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_main_delivery_unit");
    auto ods_bc_design_change_execution_copy_delivery_unit = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_copy_delivery_unit");
    auto ods_bc_design_change_execution_document_change = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_document_change");
    auto ods_bc_design_change_execution_document_change_unit = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_document_change_unit");
    auto ods_bc_design_change_execution_material_change = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_material_change");
    auto ods_bc_design_change_execution_reworked_material = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_reworked_material");
    auto ods_bc_design_change_execution_reworked_material_unit = MONGO.get_coll_data(client, "ods", "bc_design_change_execution_reworked_material_unit");

    tbb::concurrent_vector<bsoncxx::document::value, tbb::scalable_allocator<bsoncxx::document::value>> form_results;
    auto data_process = [&](const tbb::blocked_range<size_t> &range)
    {
        for (size_t index = range.begin(); index != range.end(); ++index)
        {
            nlohmann::json results_json = ods_bc_design_change_execution[index];
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
        }
    };
    if (!ods_bc_design_change_execution.empty())
    {
        LOGGER.info(this->node_name, "并行处理设计变更执行数据");
        tbb::parallel_for(tbb::blocked_range<size_t>((size_t)0, ods_bc_design_change_execution.size()), data_process);
        LOGGER.info(this->node_name, "写入设计变更执行数据");
        auto m_coll = MONGO.get_coll(client, "dwd", "业联-设计变更执行");
        if (!form_results.empty())
        {
            m_coll.drop();
            m_coll.insert_many(form_results);
        }
    }
    else
    {
        LOGGER.warn(this->node_name, "源数据为空，不更新数据");
    }
}

void task_bc_join_business_connection_close::main_logic()
{
}
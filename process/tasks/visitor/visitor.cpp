#include "nlohmann/json.hpp"

#include "visitor/visitor.hpp"

using namespace dbs;

void task_visitor::main_logic()
{
    // ----------从数据库读取数据----------
    LOGGER.info(this->node_name, "从数据库读取数据");
    auto ods_results = this->get_coll_data("ods", "submissionmodels");
    auto dm_results = this->get_coll_data("dm", "visitor_submit");

    // ----------检查数据是否更新----------
    LOGGER.info(this->node_name, "检查数据是否更新");
    // 因为访客系统的填报只能新增不会变更，所以可以根据id做增量更新
    // 提取处理完成数据的id
    tbb::concurrent_vector<std::string> dm_id;
    auto extract_id = [&](const nlohmann::json & value)
    {
        dm_id.emplace_back(value["fid"]["$oid"].get<std::string>());
    };
    tbb::parallel_for_each(dm_results.begin(), dm_results.end(), extract_id);
    // 对比需要更新的id，并将需要更新的数据拿进来
    tbb::concurrent_vector<nlohmann::json> form_results;
    auto comparison_id = [&](const nlohmann::json & value)
    {
        auto find_it = std::find(dm_id.begin(), dm_id.end(), value["_id"]["$oid"].get<std::string>());
        if (find_it == dm_id.end())
        {
            form_results.emplace_back(value);
        }
    };
    tbb::parallel_for_each(ods_results.begin(), ods_results.end(), comparison_id);
    if (form_results.empty())
    {
        LOGGER.info(this->node_name, "无数据更新");
        return;
    }

    // ----------并行处理数据----------
    LOGGER.info(this->node_name, "并行处理数据");
    // 获取单选中的值
    auto get_swich_label = [](const nlohmann::json & input_json)
    {
        std::string_view temp_id = input_json["value"]["value"].at(0);
        for (auto ch_ : input_json["properties"]["choices"])
        {
            std::string_view ch_id = ch_["id"];
            if (ch_id == temp_id)
            {
                return ch_["label"];
            }
        }
        throw std::logic_error("解析时未找到值");
    };
    // 获取是否中的值
    auto get_yesno_label = [](const nlohmann::json & input_json)
    {
        std::string_view temp_id = input_json["value"];
        for (auto ch_ : input_json["properties"]["choices"])
        {
            std::string_view ch_id = ch_["id"];
            if (ch_id == temp_id)
            {
                return ch_["label"];
            }
        }
        throw std::logic_error("解析时未找到值");
    };
    tbb::concurrent_vector<bsoncxx::document::value> visitor_submit;
    tbb::concurrent_vector<bsoncxx::document::value> visitor_submit_accompany;
    tbb::concurrent_vector<bsoncxx::document::value> visitor_submit_tutelage;
    auto data_process = [&](const nlohmann::json & input_json)
    {
        nlohmann::json results_json;
        std::vector<nlohmann::json> accompany_json;
        std::vector<nlohmann::json> tutelage_json;

        results_json["提交日期"] = input_json["updatedAt"];
        results_json["fid"] = input_json["_id"];
        for (auto ch : input_json["answers"])
        {
            std::string_view ch_title = ch["title"];
            if (ch_title == "请输入您的姓名")
            {
                results_json["提交人姓名"] = ch["value"];
            }
            else if (ch_title == "请输入您的身份证号")
            {
                results_json["提交人身份证号"] = ch["value"];
            }
            else if (ch_title == "请输入您的联系方式")
            {
                results_json["提交人电话号"] = ch["value"];
            }
            else if (ch_title == "请输入您的公司名称")
            {
                results_json["提交人所属公司"] = ch["value"];
            }
            else if (ch_title == "请选择您的作业地点")
            {
                results_json["作业地点"] = get_swich_label(ch);
            }
            else if (ch_title == "请输入您此次入场的作业人数")
            {
                results_json["作业人数"] = ch["value"];
            }
            else if (ch_title == "请输入您的计划开完工时间")
            {
                results_json["计划开工时间"] = ch["value"]["start"];
                results_json["计划完工时间"] = ch["value"]["end"];
            }
            else if (ch_title == "请选择您的作业内容大类")
            {
                results_json["作业内容大类"] = get_swich_label(ch);
            }
            else if (ch_title == "请选择作业内容细分")
            {
                results_json["作业内容细分"] = get_swich_label(ch);
            }
            else if (ch_title == "请选择作业危险性，请仔细选择")
            {
                results_json["作业危险性"] = get_swich_label(ch);
            }
            else if (ch_title == "是否需要特殊权限申请")
            {
                results_json["是否需要特殊权限申请"] = get_yesno_label(ch);
            }
            else if (ch_title == "随行人员明细")
            {
                if (ch["value"].is_array())
                {
                    size_t index = 0;
                    for (auto ch_ : ch["value"])
                    {
                        nlohmann::json temp;
                        temp["fid"] = input_json["_id"];
                        for (auto it = ch_.begin(); it != ch_.end(); it++)
                        {
                            std::string temp_label;
                            for (auto ch____ : ch["properties"]["tableColumns"])
                            {
                                if (ch____["id"] == it.key())
                                {
                                    temp_label = ch____["label"];
                                    break;
                                }
                            }
                            temp[temp_label] = ch["value"].at(index)[it.key()];
                        }
                        accompany_json.emplace_back(temp);
                        index++;
                    }
                }
            }
            else if (ch_title == "监护人明细")
            {
                if (ch["value"].is_array())
                {
                    size_t index = 0;
                    for (auto ch_ : ch["value"])
                    {
                        nlohmann::json temp;
                        temp["fid"] = input_json["_id"];
                        for (auto it = ch_.begin(); it != ch_.end(); it++)
                        {
                            std::string temp_label;
                            for (auto ch____ : ch["properties"]["tableColumns"])
                            {
                                if (ch____["id"] == it.key())
                                {
                                    temp_label = ch____["label"];
                                    break;
                                }
                            }
                            temp[temp_label] = ch["value"].at(index)[it.key()];
                        }
                        tutelage_json.emplace_back(temp);
                        index++;
                    }
                }
            }
        }
        // 插入数据
        visitor_submit.emplace_back(bsoncxx::from_json(results_json.dump()));
        for (auto ch : accompany_json)
        {
            visitor_submit_accompany.emplace_back(bsoncxx::from_json(ch.dump()));
        }
        for (auto ch : tutelage_json)
        {
            visitor_submit_tutelage.emplace_back(bsoncxx::from_json(ch.dump()));
        }
    };
    tbb::parallel_for_each(form_results.begin(), form_results.end(), data_process);

    // ----------以关系型数据库的形式写入数据库中----------
    LOGGER.info(this->node_name, "写入处理数据");
    auto m_coll = this->m_client["dm"]["visitor_submit"];
    auto m_coll_a = this->m_client["dm"]["visitor_submit_accompany"];
    auto m_coll_t = this->m_client["dm"]["visitor_submit_tutelage"];
    if (!visitor_submit.empty())
    {
        m_coll.insert_many(visitor_submit);
        if (!visitor_submit_accompany.empty())
        {
            m_coll_a.insert_many(visitor_submit_accompany);
        }
        if (!visitor_submit_tutelage.empty())
        {
            m_coll_t.insert_many(visitor_submit_tutelage);
        }
    }
}
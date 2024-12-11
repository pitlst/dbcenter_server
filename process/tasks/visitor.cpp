#include <iostream>

#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"
#include "bsoncxx/json.hpp"

#include "json.hpp"

#include "tbb/tbb.h"

#include "mongo.hpp"
#include "logger.hpp"
#include "pipeline.hpp"

int main()
{
    try
    {
        // ----------从数据库读取数据----------
        auto read_data = [](const std::string & db_name, const std::string & coll_name)
        {
            auto results_cursor = MONGO.get_db(db_name)[coll_name].find({});
            std::vector<nlohmann::json> results;
            for (auto &&ch : results_cursor)
            {
                nlohmann::json m_json = nlohmann::json::parse(bsoncxx::to_json(ch));
                results.emplace_back(m_json);
            }
            return results;
        };
        auto form_results = read_data("ods", "submissionmodels");
        auto form_model = read_data("ods", "formmodels");
        // ----------组织成二维表格的形式----------
        auto data_process = [&](size_t index)
        {
            nlohmann::json input_json = form_results[index];
            nlohmann::json results_json;


            return results_json;
        };
        

        // ----------写入数据库----------
        auto m_coll = MONGO.get_db("dm")["visitor_submit"];
        
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << '\n';
    }
    return 0;
}

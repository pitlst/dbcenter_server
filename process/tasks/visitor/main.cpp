#include <iostream>

#include "visitor.hpp"
#include "pipeline.hpp"

#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"
#include "bsoncxx/json.hpp"

int main()
{
    try
    {
        std::cout << "hello" << std::endl;
        auto result = MONGO.get_db("public")["context"].find_one({});
        std::cout << bsoncxx::to_json(*result) << std::endl;
        std::cout << "world" << std::endl;
        auto temp__ = dbs::pipeline("相关方填报数据处理");
        if (temp__.recv())
        {
            std::cout << "相关方填报数据处理" << std::endl;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << '\n';
    }
    return 0;
}
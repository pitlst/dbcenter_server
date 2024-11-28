#include <map>
#include <memory>

#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/pool.hpp"
#include "mongocxx/uri.hpp"
#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"

#include "data.hpp"
#include "mongo.hpp"

void process_visitor()
{
    // 连接数据库获取集合
    auto m_client_ptr = std::make_unique<mongocxx::pool::entry>(MONGO.m_pool_ptr->acquire());
    auto m_database_ptr = std::make_unique<mongocxx::database>((*m_client_ptr)["ods"]);
    auto collection = (*m_database_ptr)["submissionmodels"];
    auto cursor = collection.find({});
}

int main()
{
    
    return 0;
}





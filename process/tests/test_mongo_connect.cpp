#include <iostream>
#include <bsoncxx/builder/stream/document.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/cursor.hpp>

int main(int argc, char *argv[])
{
    mongocxx::instance instance;
    // 27017是默认端口
    mongocxx::uri uri{"mongodb://localhost:27017/"};
    // 创建一个client客户端
    mongocxx::client client = mongocxx::client{uri};
    mongocxx::database db = client["logger"];
    mongocxx::collection coll = db["网页自动化测试"];
    // 选择了数据库db，表coll
    auto result = coll.find_one({}).value();
    std::cout << bsoncxx::to_json(result.view()) << std::endl;
    return 0;
}
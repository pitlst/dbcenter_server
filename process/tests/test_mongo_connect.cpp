// #include <iostream>
// #include <bsoncxx/builder/stream/document.hpp>
// #include <mongocxx/instance.hpp>
// #include <bsoncxx/json.hpp>
// #include <mongocxx/client.hpp>
// #include <mongocxx/instance.hpp>
// #include <mongocxx/cursor.hpp>

// int main(int argc, char *argv[])
// try
// {
//     mongocxx::instance instance;
//     // 27017是默认端口
//     mongocxx::uri uri{"mongodb://localhost:27017/"};
//     // 创建一个client客户端
//     mongocxx::client client = mongocxx::client{uri};
//     mongocxx::database db = client["logger"];
//     mongocxx::collection coll = db["网页自动化测试"];
//     // 选择了数据库db，表coll
//     auto result = coll.find_one({}).value();
//     std::cout << bsoncxx::to_json(result.view()) << std::endl;
//     return 0;
// }
// catch(const std::exception& e)
// {
//     std::cerr << e.what() << '\n';
// }

#include <iostream>
#include <ostream>
#include <sstream>
#include <thread>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

// #include <examples/macros.hh>

using bsoncxx::builder::basic::kvp;

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::uri uri{"mongodb://localhost:27017/?minPoolSize=3&maxPoolSize=3"};

    mongocxx::pool pool{uri};

    std::vector<std::string> collection_names = {"foo", "bar", "baz"};
    std::vector<std::thread> threads{};

    for (auto i : {0, 1, 2}) {
        auto run = [&](std::int64_t j) {
            // Each client and collection can only be used in a single thread.
            auto client = pool.acquire();
            auto coll = client["test"][collection_names[static_cast<std::size_t>(j)]];
            coll.delete_many({});

            bsoncxx::types::b_int64 index = {j};
            coll.insert_one(bsoncxx::builder::basic::make_document(kvp("x", index)));

            if (auto doc =
                    client["test"][collection_names[static_cast<std::size_t>(j)]].find_one({})) {
                // In order to ensure that the newline is printed immediately after the document,
                // they need to be streamed to std::cout as a single string.
                std::stringstream ss;
                ss << bsoncxx::to_json(*doc) << std::endl;

                std::cout << ss.str();
            }

            // The client goes out of scope at the end of the lambda and is returned to the pool.
        };

        std::thread runner{run, i};

        threads.push_back(std::move(runner));
    }

    for (auto&& runner : threads) {
        runner.join();
    }
}
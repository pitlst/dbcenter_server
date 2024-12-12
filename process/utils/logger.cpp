#include <chrono>
#include <sstream>

#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/uri.hpp"

#include "bsoncxx/builder/basic/array.hpp"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/builder/basic/kvp.hpp"
#include "bsoncxx/types.hpp"

#include "fmt/core.h"
#include "fmt/chrono.h"
#include "fmt/color.h"

#include "logger.hpp"

using namespace dbs;

logger &logger::instance()
{
    static logger m_self;
    return m_self;
}

// 不同等级日志
void logger::debug(const std::string &name, const std::string &msg)
{
    return emit("DEBUG", name, msg);
}

void logger::info(const std::string &name, const std::string &msg)
{
    return emit("INFO", name, msg);
}

void logger::warn(const std::string &name, const std::string &msg)
{
    return emit("WARNNING", name, msg);
}

void logger::error(const std::string &name, const std::string &msg)
{
    return emit("ERROR", name, msg);
}

void logger::emit(const std::string &level, const std::string &name, const std::string &msg)
{
    auto now = std::chrono::system_clock::now();
    // 插入数据库
    auto collection = m_database[name];
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    collection.insert_one(
        make_document(
            kvp("timestamp", bsoncxx::types::b_date{now}),
            kvp("message", make_document(
                               kvp("等级", level),
                               kvp("消息", msg)))));
    // 命令行输出
    fmt::print(
        fmt::emphasis::bold | fg(fmt::color::cyan),
        fmt::runtime("[{}]: {:%Y-%m-%d %H:%M:%S}: {}"), level, fmt::localtime(std::chrono::system_clock::to_time_t(now)), msg
    );
}

logger::logger()
{
    // 取消c与c++共用的输出缓冲区
    std::ios::sync_with_stdio(0);
    std::cin.tie(0);
    std::cout.tie(0);
}

void logger::create_time_collection(const std::string &name)
{
    // 检查集合是否创建
    bool is_exist = false;
    auto collections = m_database.list_collections();
    for (const auto &coll : collections)
    {
        if (coll["name"].get_string().value == name)
        {
            is_exist = true;
            break;
        }
    }
    if (!is_exist)
    {
        // 创建集合
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;
        auto ts_info = make_document(
            kvp("timeseries",
                make_document(
                    kvp("timeField", "timestamp"),
                    kvp("metaField", "message"))),
            kvp("expireAfterSeconds", 604800)
        );
        m_database.create_collection(name, ts_info.view());
    }
}

std::string logger::get_time_str(const std::chrono::system_clock::time_point &input_time)
{
    std::stringstream ss;
    std::time_t tt = std::chrono::system_clock::to_time_t(input_time);
    // 使用本地时区
    std::tm tm = *std::localtime(&tt);
    ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}
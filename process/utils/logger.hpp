#ifndef DBS_LOGGER_INCLUDE
#define DBS_LOGGER_INCLUDE

#include <memory>
#include <functional>
#include <string>
#include <array>
#include <variant>

#include "oneapi/tbb.h"
#include "oneapi/tbb/tbbmalloc_proxy.h"

#include "mongo.hpp"

namespace dbs
{

    // 全局单例的日志类，用于组织日志相关的io操作
    class logger
    {
    public:
        using log_data = std::variant<std::string, std::chrono::system_clock::time_point>;

        // 获取单实例对象
        static logger &instance();
        // 不同等级日志
        void debug(const std::string &name, const std::string &msg);
        void info(const std::string &name, const std::string &msg);
        void warn(const std::string &name, const std::string &msg);
        void error(const std::string &name, const std::string &msg);
        void emit(const std::string &level, const std::string &name, const std::string &msg);
        // 真正打印与输出的地方
        void print(tbb::concurrent_map<std::string, log_data> &temp_log_data);
        std::function<void()> get_run_func();
        // 创建日志的时间序列集合，因为日志几乎不会删除，只会按条插入，所以适合于日志
        mongocxx::collection create_time_collection(const std::string &name);

    private:
        // 禁止外部构造与析构
        logger();
        ~logger() = default;

        // 通知变量
        std::mutex m_print_mtx;
        std::condition_variable m_print_cv;
        // 消息队列
        tbb::concurrent_queue<tbb::concurrent_map<std::string, log_data>> m_queue;
        // 数据库客户端
        const std::string db_name = "logger";
        mongocxx::pool::entry m_client = MONGO.init_client();
        mongocxx::database m_database = m_client[db_name];
    };
}

// log的全局引用简写
#define LOGGER dbs::logger::instance()

#endif
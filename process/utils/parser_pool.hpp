#ifndef DBS_PARSERPOOL_INCLUDE
#define DBS_PARSERPOOL_INCLUDE

#include <memory>

#include "oneapi/tbb.h"
#include "oneapi/tbb/tbbmalloc_proxy.h"

#include "simdjson.h"

namespace dbs
{
    // simdjson的解析器池，用于在多线程环境复用解析器和其缓冲区
    // 使用全局单例
    class parser_pool
    {
    public:
        // 获取单实例对象
        static parser_pool &instance();

        // 获取解析器
        std::unique_ptr<simdjson::dom::parser> acquire();
        // 释放解析器
        void release(std::unique_ptr<simdjson::dom::parser> parser);
        // 解析字符串的快捷操作
        simdjson::dom::document parse_json(simdjson::padded_string &json_str) const;

    private:
        // 禁止外部构造与析构
        parser_pool();
        ~parser_pool();

        tbb::concurrent_queue<std::unique_ptr<simdjson::dom::parser>> m_pool;
    };
}

// 解析器池全局单例的简写
#define PARSERPOOL dbs::parser_pool::instance()

#endif
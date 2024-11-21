#ifndef DBS_NODE_INCLUDE
#define DBS_NODE_INCLUDE

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>

#include "json.hpp"
using json = nlohmann::json;

#include "node.hpp"
#include "general.hpp"

namespace dbs
{
    // 节点的定义
    struct node
    {
        // 节点名称
        std::string name;
        // 节点的执行方法
        std::function<void()> func;
        // 节点依赖的节点名称
        std::unordered_set<std::string> deps;
    };

    // 获取所有的节点的定义
    std::unordered_set<node> get_total_node_define(const json & input_node_define);
    // 检查已经定义的节点的问题，并生成对应的报告字符串
    std::string check_node(const std::unordered_set<node> & input_node);
}

// 哈希支持，使node支持哈希，从而支持underorder_set一类的无序容器
namespace std
{
    template <> 
    struct hash<dbs::node>
    {
    public:
        size_t operator()(const dbs::node &node_) const
        {
            // 将节点的名称与依赖拼在一起作为哈希用的唯一项
            std::string temp_hash = node_.name;
            for(const auto & ch : node_.deps)
            {
                temp_hash += ch;
            }
            // 增加随机字符串防止空节点装桶
            temp_hash += dbs::random_gen(8);
            return hash<std::string>()(temp_hash);
        }
    };
};

#endif
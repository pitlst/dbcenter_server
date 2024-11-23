#ifndef DBS_NODE_INCLUDE
#define DBS_NODE_INCLUDE

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <future>
#include <any>
#include <atomic>

#include "json.hpp"
using json = nlohmann::json;

#include "general.hpp"

namespace dbs
{
    // 节点的基类定义
    struct node
    {
        // 节点名称
        std::string name;
        // 节点依赖的节点名称
        std::unordered_set<std::string> deps;
        // 节点执行的逻辑
        // 在这里要求所有的节点都不能有函数上下文的输入输出值
        // 这些数据的传递要么通过mongo的中转数据库，要么通过全局共享的线程安全原子变量传递，通过将变量的生命周期扩展到全程序来保证变量的生命周期安全
        std::function<void()> func;
    };

    // 因为lambda函数的实现不限制作用域，所以之后要求所有的节点实际实现全部放在头文件中方便引用
    // 同时要求节点必须在dbs命名空间中
    // 这里用于预定义函数上下文传递的全局线程安全的变量，一定要做好人工检查，因为没有编译器检查了
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
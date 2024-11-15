#ifndef DBS_NODE_INCLUDE
#define DBS_NODE_INCLUDE

#include <string>
#include <vector>
#include <functional>

#include "json.hpp"
using json = nlohmann::json;

namespace dbs
{
    // 调度节点的基类
    // 在继承后需要编写括号的重载，在其中编写真正的逻辑
    struct node_base
    {
        node_base(const json & node_define);
        node_base(const std::string &name, const std::string &type, const std::vector<std::string> &next_name);
        // 为节点填充可执行对象
        virtual node_base &emplace(std::function<void()> input_warpper) final;

        // 数据来源和目标的配置
        json source_config;
        json target_config; 
        // 节点名称
        std::string name;
        // 节点类型
        std::string type;
        // 节点的依赖
        std::vector<std::string> next_name;
        // 包装好的函数指针，也就是函数实际执行的位置
        std::function<void()> func_warpper;
    };

    // sql节点，用于运行sql
    // 注意，常用复杂语句应当通过sql设计为存储过程，然后在sql中调用存储过程即可
    class sql_node final: public node_base 
    {
    public:
        sql_node(const json & node_define);
        void operator()();

    private:
        // 读取进来执行的sql
        std::string m_sql;
    };

    // mongo的js查询节点，用于运行查询脚本
    class mongojs_node final: public node_base 
    {
    public:
        mongojs_node(const json & node_define);
        void operator()();

    private:
        // 读取进来执行的js
        std::string m_js;
    };
}

#endif
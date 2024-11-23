#ifndef DBS_DAG_INCLUDE
#define DBS_DAG_INCLUDE

#include <vector>
#include <unordered_set>

#include "toml.hpp"

#include "node.hpp"

namespace dbs
{
    class dag_scheduler
    {
    public:
        dag_scheduler(const toml::value & config_define);
        ~dag_scheduler();
        // 真正运行并派发节点的地方
        void run();

    private:
        // 获取当前执行的节点
        std::vector<node> make_node();
        // 接收并处理ipc通信
        void get_notice();
        // 对当前的节点做拓扑排序，保证先执行的节点先被推进线程池

    private:
        // 任务轮询也就是数据同步的最小间隔，单位秒
        std::size_t min_sync_interval;
        // 无任务时等待时间，单位秒
        std::size_t wait_sync_interval;
        // socket通信的未完全接收的节点信息缓存
        std::string socket_buffer;
        // 所有节点和他的被依赖关系，注意这里的依赖关系和tasks.json文件中定义的是相反的
        std::unordered_map<std::string, std::unordered_set<std::string>> node_deps;
    };
}

#endif
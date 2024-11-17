#ifndef DBS_DAG_INCLUDE
#define DBS_DAG_INCLUDE

#include "toml.hpp"

#include "thread_pool.hpp"

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
        // 处理节点的依赖
        void process_name();
        // 获取当前可以执行的节点
        





    };
}

#endif
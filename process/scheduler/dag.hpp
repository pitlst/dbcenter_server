#ifndef DBS_DAG_INCLUDE
#define DBS_DAG_INCLUDE



#include "thread_pool.hpp"

namespace dbs
{


    class dag_scheduler
    {
    public:
        // 真正运行并派发节点的地方
        void run();
        // 包装当前节点成std::function交给线程池调度

    private:
        // 处理节点的依赖
        void process_name();
        // 获取当前可以执行的节点
        



    };
}

#endif
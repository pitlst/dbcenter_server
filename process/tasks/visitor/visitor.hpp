#ifndef DBS_TASK_VISITOR_INCLUDE
#define DBS_TASK_VISITOR_INCLUDE

#include "base.hpp"

namespace dbs
{
    class task_visitor : public task_base
    {
    public:
        task_visitor(): task_base("外网访客系统数据处理") {}
        ~task_visitor() = default;
    private:
        void main_logic() override;
    };
}

#endif


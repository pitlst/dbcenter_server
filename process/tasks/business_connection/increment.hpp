#ifndef DBS_TASK_BC_JOIN_INCLUDE
#define DBS_TASK_BC_JOIN_INCLUDE

#include "base.hpp"

namespace dbs
{
    class task_bc_join_class_group: public task_base
    {
    public:
        task_bc_join_class_group() : task_base("业联系统数据处理-拼接班组") {}
        ~task_bc_join_class_group() = default;

    private:
        void main_logic() override;
    };
}

#endif

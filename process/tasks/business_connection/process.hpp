#ifndef DBS_TASK_BC_PROCESS_INCLUDE
#define DBS_TASK_BC_PROCESS_INCLUDE

#include "base.hpp"

namespace dbs
{
    class task_bc_process : public task_base
    {
    public:
        task_bc_process(): task_base("业联系统数据处理") {}
        ~task_bc_process() = default;
    private:
        void main_logic() override;
    };
}

#endif

#ifndef DBS_TASK_BC_JOIN_INCLUDE
#define DBS_TASK_BC_JOIN_INCLUDE

#include "base.hpp"

namespace dbs
{
    class task_bc_join : public task_base
    {
    public:
        task_bc_join(): task_base("业联系统数据处理-拼接") {}
        ~task_bc_join() = default;
    private:
        void main_logic() override;

        void logic_class_group();
        void logic_technological_process();
        void logic_business_connection();
        void logic_design_change();
        void logic_shop_execution();
        void logic_design_change_execution();

        tbb::task_group tg;
    };
}

#endif

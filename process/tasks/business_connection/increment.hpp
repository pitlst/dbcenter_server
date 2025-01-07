#ifndef DBS_TASK_BC_JOIN_INCLUDE
#define DBS_TASK_BC_JOIN_INCLUDE

#include "base.hpp"

namespace dbs
{
    class increment_class_group: public task_base
    {
    public:
        increment_class_group() : task_base("业联系统增量检查-班组") {}
        ~increment_class_group() = default;

    private:
        void main_logic() override;
    };

    class increment_business_connection: public task_base
    {
    public:
        increment_business_connection() : task_base("业联系统增量检查-业务联系书") {}
        ~increment_business_connection() = default;

    private:
        void main_logic() override;
    };

    class increment_design_change: public task_base
    {
    public:
        increment_design_change() : task_base("业联系统增量检查-设计变更") {}
        ~increment_design_change() = default;

    private:
        void main_logic() override;
    };

    class increment_technological_process: public task_base
    {
    public:
        increment_technological_process() : task_base("业联系统增量检查-工艺流程") {}
        ~increment_technological_process() = default;

    private:
        void main_logic() override;
    };

    class increment_shop_execution: public task_base
    {
    public:
        increment_shop_execution() : task_base("业联系统增量检查-车间执行单") {}
        ~increment_shop_execution() = default;

    private:
        void main_logic() override;
    };

    class increment_design_change_execution: public task_base
    {
    public:
        increment_design_change_execution() : task_base("业联系统增量检查-设计变更执行") {}
        ~increment_design_change_execution() = default;

    private:
        void main_logic() override;
    };
}

#endif

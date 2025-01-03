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

    class task_bc_join_technological_process: public task_base
    {
    public:
        task_bc_join_technological_process() : task_base("业联系统数据处理-拼接工艺流程") {}
        ~task_bc_join_technological_process() = default;

    private:
        void main_logic() override;
    };

    class task_bc_join_business_connection: public task_base
    {
    public:
        task_bc_join_business_connection() : task_base("业联系统数据处理-拼接业务联系书") {}
        ~task_bc_join_business_connection() = default;

    private:
        void main_logic() override;
    };

    class task_bc_join_design_change: public task_base
    {
    public:
        task_bc_join_design_change() : task_base("业联系统数据处理-拼接设计变更") {}
        ~task_bc_join_design_change() = default;

    private:
        void main_logic() override;
    };

    class task_bc_join_shop_execution: public task_base
    {
    public:
        task_bc_join_shop_execution() : task_base("业联系统数据处理-拼接车间执行单") {}
        ~task_bc_join_shop_execution() = default;

    private:
        void main_logic() override;
    };

    class task_bc_join_design_change_execution: public task_base
    {
    public:
        task_bc_join_design_change_execution() : task_base("业联系统数据处理-拼接设计变更执行") {}
        ~task_bc_join_design_change_execution() = default;

    private:
        void main_logic() override;
    };


    class task_bc_join_business_connection_close: public task_base
    {
    public:
        task_bc_join_business_connection_close() : task_base("业联系统数据处理-拼接业联关闭") {}
        ~task_bc_join_business_connection_close() = default;

    private:
        void main_logic() override;
    };
}

#endif

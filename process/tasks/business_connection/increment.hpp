#ifndef DBS_TASK_BC_INCREMENT_INCLUDE
#define DBS_TASK_BC_INCREMENT_INCLUDE

#include "base.hpp"

namespace dbs
{

    class task_bc_increment: public task_base
    {
    public:
        task_bc_increment(const std::string & node_name) : task_base(node_name) {}
        ~task_bc_increment() = default;

    protected:
        void sql_make(const std::string &file_name, const tbb::concurrent_vector<std::string> & request_id, const tbb::concurrent_vector<std::string> &other_if = tbb::concurrent_vector<std::string>{}) const;
    };

    class increment_class_group: public task_bc_increment
    {
    public:
        increment_class_group() : task_bc_increment("业联系统增量检查-班组") {}
        ~increment_class_group() = default;

    private:
        void main_logic() override;
    };

    class increment_business_connection: public task_bc_increment
    {
    public:
        increment_business_connection() : task_bc_increment("业联系统增量检查-业务联系书") {}
        ~increment_business_connection() = default;

    private:
        void main_logic() override;
    };

    class increment_design_change: public task_bc_increment
    {
    public:
        increment_design_change() : task_bc_increment("业联系统增量检查-设计变更") {}
        ~increment_design_change() = default;

    private:
        void main_logic() override;
    };

    class increment_technological_process: public task_bc_increment
    {
    public:
        increment_technological_process() : task_bc_increment("业联系统增量检查-工艺流程") {}
        ~increment_technological_process() = default;

    private:
        void main_logic() override;
    };

    class increment_shop_execution: public task_bc_increment
    {
    public:
        increment_shop_execution() : task_bc_increment("业联系统增量检查-车间执行单") {}
        ~increment_shop_execution() = default;

    private:
        void main_logic() override;
    };

    class increment_design_change_execution: public task_bc_increment
    {
    public:
        increment_design_change_execution() : task_bc_increment("业联系统增量检查-设计变更执行") {}
        ~increment_design_change_execution() = default;

    private:
        void main_logic() override;
    };

    class increment_business_connection_close: public task_bc_increment
    {
    public:
        increment_business_connection_close() : task_bc_increment("业联系统增量检查-业联执行关闭") {}
        ~increment_business_connection_close() = default;

    private:
        void main_logic() override;
    };
}

#endif

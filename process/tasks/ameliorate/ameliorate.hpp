#ifndef DBS_TASK_AMELIORATE_INCLUDE
#define DBS_TASK_AMELIORATE_INCLUDE

#include "base.hpp"

namespace dbs
{
    class increment_ameliorate: public task_base
    {
    public:
        increment_ameliorate() : task_base("改善增量检查") {}
        ~increment_ameliorate() = default;

    private:
        void main_logic() override;
    };

    class task_ameliorate: public task_base
    {
    public:
        task_ameliorate() : task_base("改善数据清洗") {}
        ~task_ameliorate() = default;

    private:
        void main_logic() override;
    };

    class task_ameliorate_process: public task_base
    {
    public:
        task_ameliorate_process() : task_base("改善数据处理") {}
        ~task_ameliorate_process() = default;

    private:
        void main_logic() override;
    };
}

#endif
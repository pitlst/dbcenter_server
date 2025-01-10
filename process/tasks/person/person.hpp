#ifndef DBS_TASK_PERSON_INCLUDE
#define DBS_TASK_PERSON_INCLUDE

#include "base.hpp"

namespace dbs
{
    class increment_person : public task_base
    {
    public:
        increment_person(): task_base("人员数据增量检查") {}
        ~increment_person() = default;
    private:
        void main_logic() override;
    };

    class task_person : public task_base
    {
    public:
        task_person(): task_base("人员数据清洗") {}
        ~task_person() = default;
    private:
        void main_logic() override;
    };
}

#endif

#ifndef DBS_TASK_PERSON_INCLUDE
#define DBS_TASK_PERSON_INCLUDE

#include "base.hpp"

namespace dbs
{
    class task_person : public task_base
    {
    public:
        task_person(): task_base("人员基础数据统计") {}
        ~task_person() = default;
    private:
        void main_logic() override;
    };
}

#endif

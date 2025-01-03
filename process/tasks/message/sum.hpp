#ifndef DBS_TASK_MSG_SUM_INCLUDE
#define DBS_TASK_MSG_SUM_INCLUDE

#include "base.hpp"

namespace dbs
{
    class task_msg_sum : public task_base
    {
    public:
        task_msg_sum(): task_base("短信数据处理-薪酬求和") {}
        ~task_msg_sum() = default;
    private:
        void main_logic() override;
    };
}

#endif

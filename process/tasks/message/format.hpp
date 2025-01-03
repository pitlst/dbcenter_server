#ifndef DBS_TASK_MSG_FORMAT_INCLUDE
#define DBS_TASK_MSG_FORMAT_INCLUDE

#include "base.hpp"

namespace dbs
{
    class task_msg_format : public task_base
    {
    public:
        task_msg_format(): task_base("短信数据处理-薪酬提取") {}
        ~task_msg_format() = default;
    private:
        void main_logic() override;
    };
}

#endif

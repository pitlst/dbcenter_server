#ifndef DBS_SOCKET_INCLUDE
#define DBS_SOCKET_INCLUDE

#include "toml.hpp"

namespace dbs
{
    class socket
    {
    public:
        // 获取单实例对象
        static socket &instance();
    private:
        socket();
        ~socket();
    };

    
}


#endif
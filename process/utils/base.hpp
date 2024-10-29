#ifndef DBS_BASE_INCLUDE
#define DBS_BASE_INCLUDE

#include <string>
#include <random>

namespace dbs
{
    class no_copy
    {
    public:
        ~no_copy() = default;

    protected:
        no_copy() = default;

    private:
        no_copy(const no_copy &) = delete;
        no_copy &operator=(const no_copy &) = delete;
    };
}

#endif
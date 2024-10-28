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

    // 生成一个随机字符串
    inline std::string random_gen(int length)
    {
        char tmp;
        std::string buffer; // buffer: 保存返回值
        std::random_device rd;
        std::default_random_engine random(rd());
        for (int i = 0; i < length; i++)
        {
            tmp = random() % 36; // 随机一个小于 36 的整数，0-9、A-Z 共 36 种字符
            if (tmp < 10)
            { // 如果随机数小于 10，变换成一个阿拉伯数字的 ASCII
                tmp += '0';
            }
            else
            { // 否则，变换成一个大写字母的 ASCII
                tmp -= 10;
                tmp += 'A';
            }
            buffer += tmp;
        }
        return buffer;
    }
}

#endif
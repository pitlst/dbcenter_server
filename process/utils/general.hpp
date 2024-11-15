#ifndef DBS_GENERAL_INCLUDE
#define DBS_GENERAL_INCLUDE

#include <string>

namespace dbs
{ 
    // 生成一个随机字符串
    std::string random_gen(int length);
    // 将文件读进字符串
    std::string read_file(const std::string & input_path);
    // 将gbk字符串转换为utf8
    std::string gbk_to_utf8(const std::string & input_str);
}

#endif
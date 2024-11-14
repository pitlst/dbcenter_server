#ifndef DBS_GENERAL_INCLUDE
#define DBS_GENERAL_INCLUDE

#include <string>

#include <filesystem>

namespace dbs
{ 
    // 生成一个随机字符串
    std::string random_gen(int length);
    // 将文件读进字符串
    std::string read_file(std::filesystem::path input_path);

}

#endif
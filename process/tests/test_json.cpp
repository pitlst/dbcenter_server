#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cassert>

#include "utils/json.hpp"

int main()
{
    std::cout << "hello" << std::endl;

    dbs::json tmep(true);
    std::cout << tmep.to_str().value() << std::endl;
    std::cout << tmep.to_bool().value() << std::endl;
    std::cout << tmep.to_int().value() << std::endl;
    std::cout << tmep.to_double().value() << std::endl;
    dbs::json tempp(2.);
    std::cout << tempp.to_str().value() << std::endl;
    std::cout << tempp.to_bool().value() << std::endl;
    std::cout << tempp.to_int().value() << std::endl;
    std::cout << tempp.to_double().value() << std::endl;
    dbs::json temp2 = dbs::json::array({0, 1, 2});
    std::cout << temp2.to_str().value() << std::endl;
    // std::cout << temp2.to_bool() << std::endl;
    // std::cout << temp2.to_int() << std::endl;
    dbs::json tmep3;
    std::cout << tmep3.to_str().value() << std::endl;

    std::string test_json;
    std::stringstream ss;
    std::ifstream infile;

    // infile.open(std::string(R"(C:\Users\cheakf\Documents\workspace\dbcenter_server\test\source\test.json)")); // 将文件流对象与文件连接起来
    infile.open(std::string(PROJECT_PATH) + "source/test.json");
    assert(infile.is_open());                                                                                 // 若失败,则输出错误消息,并终止程序运行

    char c;
    // infile >> noskipws;
    while (!infile.eof())
    {
        infile >> c;
        ss << c;
    }
    infile.close(); // 关闭文件输入流
    test_json = ss.str();
    std::cout << test_json << std::endl;

    dbs::json_parse temp_parse;
    temp_parse.load(test_json);
    dbs::json temp = temp_parse.parse();
    std::cout << dbs::format_json(temp.to_str().value()) << std::endl;
    return 0;
    return 0;
}
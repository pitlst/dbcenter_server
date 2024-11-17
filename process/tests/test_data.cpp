#include <iostream>
#include <vector>
#include "toml.hpp"
#include "utils/data.hpp"

int main()
{
    std::cout << "hello" << std::endl;
    auto data = toml::parse("example.toml", toml::spec::v(1,1,0));
    return 0;
} 
#include <iostream>

#include "dag.hpp"

int main()
{
    std::cout << "process开始运行" << std::endl;
    try
    {
        auto dag = dbs::dag_scheduler();
        dag.run();
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    return 0;
}

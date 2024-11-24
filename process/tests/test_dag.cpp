#include <iostream>
#include <vector>
#include <functional>

#include "thread_pool.hpp"
#include "dag.hpp"

// int main()
// {
    
// }

class demo
{
public:
    int test(int n)
    {
        int num = 0;
        for (int i = 0; i < n; i++)
        {

            std::cout << "hello " << i << std::endl;

            num = i * i;
        };
        return num;
    }
};


int main()
{
    std::vector<std::future<int>> results;

    // using namespace std::placeholders;
    int i = 8;

    demo d;
    std::function<int(int)> f = std::bind(&demo::test, &d, 1);
    results.emplace_back(dbs::thread_pool::instance().submit_lambda(f, i));

    for (auto &&result : results)
        std::cout << result.get() << ' ';
    std::cout << std::endl;

    return 0;
}
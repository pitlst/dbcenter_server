#ifndef DBS_NODE_PERSON_INCLUDE
#define DBS_NODE_PERSON_INCLUDE

#include "node.hpp"
#include <iostream>

namespace dbs
{
    inline node make_person_node(const std::string & node_name)
    {
        auto process_person = []()
        {
            std::cout << "person已执行" << std::endl;
        };

        node person;
        person.name = node_name;
        person.func = process_person;
        return person;
    }
}

#endif
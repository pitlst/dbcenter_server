#include <iostream>
#include "mysqlx/xdevapi.h"

int main()
{
    std::cout << "hello" << std::endl;
    mysqlx::Session sess("localhost", 33060, "root", "123456");
    sess.sql("USE dataframe_flow_v2").execute();
    auto myResult = sess.sql("SELECT * from ods_shr_staff").execute();
    mysqlx::Row row = myResult.fetchOne();
    std::cout << row[0] << std::endl;
    return 0;
}

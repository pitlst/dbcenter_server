#include "nanodbc/nanodbc.h"
#include <iostream>
#include <exception>

int main()
try
{
    std::cout << "hello" << std::endl;
    // auto const connstr = NANODBC_TEXT("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=localhost;Port=5432;Database=postgres;UID=postgres;PWD=123456"); // an ODBC connection string to your database
    auto const connstr = NANODBC_TEXT("Driver={MySQL ODBC 9.1 Unicode Driver};Server=localhost;Port=3306;Database=dataframe_flow_v2;UID=root;PWD=123456");
    nanodbc::connection conn(connstr);
    // nanodbc::execute(conn, NANODBC_TEXT("create table t (i int)"));
    // nanodbc::execute(conn, NANODBC_TEXT("insert into t (1)"));
    std::cout << "world" << std::endl;

    auto result = nanodbc::execute(conn, NANODBC_TEXT("SELECT * FROM dm_abnormal"));
    while (result.next())
    {
        auto i = result.get<std::string>("id");
        std::cout << i << std::endl;
    }
    return EXIT_SUCCESS;
}
catch (std::exception &e)
{
    std::cerr << e.what() << std::endl;
    return EXIT_FAILURE;
}
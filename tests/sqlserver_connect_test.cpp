#include <iostream>

#include <windows.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>

int main()
{
    SQLHANDLE sqlEnvHandle = nullptr;
    SQLHANDLE sqlConnHandle = nullptr;
    if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlEnvHandle))
    {
        throw std::logic_error("未设置成功");
    }
    
    SQLCHAR connectionString[1024] = "DRIVER={SQL Server};SERVER=<server_name>;DATABASE=dataframe;UID=sa;PWD=Swq8855830.;";
    if (SQL_SUCCESS != SQLDriverConnect(sqlConnHandle, NULL, connectionString, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT))
    {
        throw std::logic_error("未连接成功");
    }
    SQLHANDLE sqlStmtHandle;
    if (SQL_SUCCESS != SQLAllocHandle(SQL_HANDLE_STMT, sqlConnHandle, &sqlStmtHandle))
    {
        throw std::logic_error("创建语句错误");
    }
    SQLDisconnect(sqlConnHandle);
    SQLFreeHandle(SQL_HANDLE_STMT, sqlStmtHandle);
    SQLFreeHandle(SQL_HANDLE_ENV, sqlEnvHandle);
    std::cout << "hello" << std::endl;
    return 0;
}
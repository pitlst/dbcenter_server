#--------------------------------------------------------------------------------------#
#   项目总体设置
#--------------------------------------------------------------------------------------#
cmake_minimum_required(VERSION 3.12)
# 设置项目名称
set(PROJECT_NAME dbcenter_server)
# 在这里直接指定项目为c++项目，节约查找tool chain的时间
project(${PROJECT_NAME} VERSION 1.0.0 LANGUAGES CXX)
# 设置c++标准等级
set(CMAKE_CXX_STANDARD 20)
# 导出执行命令到json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# 强制化c++标准，当标准不符合要求时强制终止编译而不是采用更旧的标准
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# 判断项目平台
if(CMAKE_HOST_SYSTEM_NAME MATCHES "Windows")
    message(STATUS ${CMAKE_HOST_SYSTEM_NAME} " 检测到为windows平台")
else()
	message(FATAL_ERROR ${CMAKE_HOST_SYSTEM_NAME} " 检测到不是Windows平台,该平台目前未支持") 
endif()
# 判断编译类型
if (NOT CMAKE_BUILD_TYPE)
   set (CMAKE_BUILD_TYPE "Release")
   message (STATUS "没有编译类型选择, 默认选择 ${CMAKE_BUILD_TYPE}, 注意非release会导致动态库加载失败")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    message(FATAL_ERROR "编译类型选择 ${CMAKE_BUILD_TYPE}, 但是该编译类型不支持") 
endif ()

#--------------------------------------------------------------------------------------#
#   项目依赖设置
#--------------------------------------------------------------------------------------#
# mysql数据库连接
set(mysql-concpp_DIR "${PROJECT_SOURCE_DIR}/3rd_party/mysql-concpp")
set(MYSQL_CONCPP_LIBRARIES
    ${mysql-concpp_DIR}/lib64/vs14/libcrypto.lib
    ${mysql-concpp_DIR}/lib64/vs14/libssl.lib
    ${mysql-concpp_DIR}/lib64/vs14/mysqlcppconn.lib
    ${mysql-concpp_DIR}/lib64/vs14/mysqlcppconnx.lib
)
set(MYSQL_CONCPP_DLLS
   "${mysql-concpp_DIR}/lib64/libcrypto-3-x64.dll"
   "${mysql-concpp_DIR}/lib64/libssl-3-x64.dll"
   "${mysql-concpp_DIR}/lib64/mysqlcppconn-10-vs14.dll"
   "${mysql-concpp_DIR}/lib64/mysqlcppconnx-2-vs14.dll"
)
find_package(mysql-concpp REQUIRED)

# mongo数据库连接
set(mongo_DIR "${PROJECT_SOURCE_DIR}/3rd_party/mongo-cxx")
set(bson-1.0_DIR "${mongo_DIR}/lib/cmake/bson-1.0")
set(bsoncxx_DIR "${mongo_DIR}/lib/cmake/bsoncxx-3.11.0")
set(mongoc-1.0_DIR "${mongo_DIR}/lib/cmake/mongoc-1.0")
set(mongocxx_DIR "${mongo_DIR}/lib/cmake/mongocxx-3.11.0")
set(MONGO_CONCPP_DLLS
    ${mongo_DIR}/bin/bson-1.0.dll
    ${mongo_DIR}/bin/bsoncxx-v_noabi-rhs-x64-v143-md.dll
    ${mongo_DIR}/bin/concrt140.dll
    ${mongo_DIR}/bin/mongoc-1.0.dll
    ${mongo_DIR}/bin/mongocxx-v_noabi-rhs-x64-v143-md.dll
    ${mongo_DIR}/bin/msvcp140.dll
    ${mongo_DIR}/bin/msvcp140_1.dll
    ${mongo_DIR}/bin/msvcp140_2.dll
    ${mongo_DIR}/bin/msvcp140_atomic_wait.dll
    ${mongo_DIR}/bin/vcruntime140.dll
    ${mongo_DIR}/bin/vcruntime140_1.dll
)
find_package(bsoncxx REQUIRED)
find_package(mongocxx REQUIRED)



# 设置头文件查找路径
include_directories(
    ${mysql-concpp_DIR}/include
    ${LIBMONGOCXX_INCLUDE_DIR}
    ${LIBBSONCXX_INCLUDE_DIR}
    utils
    connect
    logger
    watch_dog
    scheduler
    tasks    
)
#--------------------------------------------------------------------------------------#
#   项目编译设置
#--------------------------------------------------------------------------------------#
# 把项目目录定义成宏传递给程序
add_compile_definitions(PROJECT_PATH=\"${PROJECT_SOURCE_DIR}/\")
add_executable(${PROJECT_NAME} 
                main.cpp 
                utils/base.hpp
                utils/data.hpp
                utils/data.cpp
                utils/json.hpp
                utils/json.cpp
                utils/big_int.hpp
                utils/big_int.cpp
                connect/connect.hpp
                connect/connect.cpp
                connect/mongo.hpp
                connect/mongo.cpp
                connect/mysql.hpp
                connect/mysql.cpp
                connect/oracle.hpp
                connect/oracle.cpp
                connect/sqlserver.hpp
                connect/sqlserver.cpp
                connect/postgresql.hpp
                connect/postgresql.cpp
                watch_dog/watch_dog.hpp
                watch_dog/watch_dog.cpp
                logger/logger.hpp
                logger/logger.cpp
                scheduler/dag.hpp
                scheduler/dag.cpp
                scheduler/node.hpp
                scheduler/node.cpp
                scheduler/thread_pool.hpp
                scheduler/thread_pool.cpp
                tasks/process_error.hpp
                tasks/process_error.cpp
)
target_link_libraries(${PROJECT_NAME} ${MYSQL_CONCPP_LIBRARIES})


#--------------------------------------------------------------------------------------#
#   测试执行测试
#--------------------------------------------------------------------------------------#
# mysql链接测试
add_executable(mysql_connect_test 
                tests/mysql_connect_test.cpp 
)
target_link_libraries(mysql_connect_test ${MYSQL_CONCPP_LIBRARIES})
foreach(FILE_I ${MYSQL_CONCPP_DLLS})
    add_custom_command(
        TARGET mysql_connect_test 
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} 
        ARGS -E copy_if_different ${FILE_I} "${PROJECT_SOURCE_DIR}/build/Release"
    )
endforeach(FILE_I)

# mongo链接测试
add_executable(mongo_connect_test 
                tests/mongo_connect_test.cpp 
)
target_link_libraries(mongo_connect_test mongo::mongocxx_shared)
foreach(FILE_I ${MONGO_CONCPP_DLLS})
    add_custom_command(
        TARGET mongo_connect_test 
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} 
        ARGS -E copy_if_different ${FILE_I} "${PROJECT_SOURCE_DIR}/build/Release"
    )
endforeach(FILE_I)
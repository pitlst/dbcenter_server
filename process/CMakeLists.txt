#--------------------------------------------------------------------------------------#
#   项目总体设置
#--------------------------------------------------------------------------------------#
cmake_minimum_required(VERSION 3.12)
# 设置项目名称
set(PROJECT_NAME dbcenter_server)
# 在这里直接指定项目为c++项目，节约查找tool chain的时间
project(${PROJECT_NAME} VERSION 1.0.0 LANGUAGES CXX)
# 设置c++标准等级为17
set(CMAKE_CXX_STANDARD 17)
# 导出执行命令到json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# 强制化c++标准，当标准不符合要求时强制终止编译而不是采用更旧的标准
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# 判断项目平台
if(CMAKE_HOST_SYSTEM_NAME MATCHES "Windows")
    message(STATUS ${CMAKE_HOST_SYSTEM_NAME} " 检测到为windows平台")
    set(PLATFORM_WINDOWS "TRUE")
    # 解决msvc默认不为utf8解码源代码文件的问题
    add_compile_options("$<$<C_COMPILER_ID:MSVC>:/utf-8>")
    add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")
else()
    set(PLATFORM_WINDOWS "FALSE")
	message(FATAL_ERROR ${CMAKE_HOST_SYSTEM_NAME} " 检测到不是Windows平台,该平台目前未支持") 
endif()
# 判断编译类型
if (NOT CMAKE_BUILD_TYPE)
   set (CMAKE_BUILD_TYPE "Release")
   message (STATUS "没有编译类型选择, 默认选择 ${CMAKE_BUILD_TYPE}, 注意非release会导致动态库加载失败")
endif()

#--------------------------------------------------------------------------------------#
#   项目依赖设置
#--------------------------------------------------------------------------------------#
# 设置头文件查找路径
# 头文件模式集成json库和toml库
include_directories(
    ${PROJECT_SOURCE_DIR}
    ${LIBMONGOCXX_INCLUDE_DIR}
    ${LIBBSONCXX_INCLUDE_DIR}
    ${ODBC_INCLUDE_DIRS}
    3rd_party/nlohmann
    3rd_party/toml11
    utils
    connect
    logger
    process_manage
    scheduler
    tasks
)

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

# SOCI连接配置
set(SOCI_DIR "${PROJECT_SOURCE_DIR}/3rd_party/soci/lib/cmake/SOCI")
# https://stackoverflow.com/questions/26822014/soci-external-symbol-unresolved-struct-socimysql-backend-factory-const-soci
# 用于解决odbc_option_driver_complete未定义问题
add_compile_definitions(SOCI_DLL)
set(SOCI_DLLS
    ${SOCI_DIR}/../../../bin/soci_core_4_0.dll
    ${SOCI_DIR}/../../../bin/soci_empty_4_0.dll
    ${SOCI_DIR}/../../../bin/soci_odbc_4_0.dll
)
find_package(SOCI REQUIRED)

#--------------------------------------------------------------------------------------#
#   项目编译设置
#--------------------------------------------------------------------------------------#
# 把项目目录定义成宏传递给程序
add_compile_definitions(PROJECT_PATH=\"${PROJECT_SOURCE_DIR}/\")
# 解决msvc的C4996警告
add_compile_definitions(_CRT_SECURE_NO_WARNINGS=1)
add_executable(${PROJECT_NAME} 
                main.cpp
                utils/general.hpp
                utils/general.cpp
                utils/data.cpp
                utils/data.cpp
                connect/odbc.hpp
                connect/odbc.cpp
                connect/mongo.hpp
                connect/mongo.cpp
                process_manage/watch_dog.hpp
                process_manage/watch_dog.cpp
                process_manage/host.hpp
                process_manage/host.cpp
                logger/logger.hpp
                logger/logger.cpp
                scheduler/dag.hpp
                scheduler/dag.cpp
                scheduler/thread_pool.hpp
                scheduler/thread_pool.cpp
                tasks/node.hpp
                tasks/node.cpp
                tasks/process/travel.hpp
                tasks/process/travel.cpp
                tasks/process/error.hpp
                tasks/process/error.cpp
)
target_link_libraries(${PROJECT_NAME} SOCI::soci_core SOCI::soci_odbc mongo::mongocxx_shared)
foreach(FILE_I ${MONGO_CONCPP_DLLS})
    add_custom_command(
        TARGET ${PROJECT_NAME} 
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} 
        ARGS -E copy_if_different ${FILE_I} "${PROJECT_SOURCE_DIR}/build/Release"
    )
endforeach(FILE_I)
foreach(FILE_I ${SOCI_DLLS})
    add_custom_command(
        TARGET ${PROJECT_NAME}  
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} 
        ARGS -E copy_if_different ${FILE_I} "${PROJECT_SOURCE_DIR}/build/Release"
    )
endforeach(FILE_I)

#--------------------------------------------------------------------------------------#
#   测试执行测试
#--------------------------------------------------------------------------------------#
# odbc测试
add_executable(test_odbc 
                tests/test_odbc.cpp
)
target_link_libraries(test_odbc SOCI::soci_core SOCI::soci_odbc)
foreach(FILE_I ${SOCI_DLLS})
    add_custom_command(
        TARGET test_odbc 
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} 
        ARGS -E copy_if_different ${FILE_I} "${PROJECT_SOURCE_DIR}/build/Release"
    )
endforeach(FILE_I)

# mongo链接测试
add_executable(test_mongo_connect 
                tests/test_mongo_connect.cpp 
)
target_link_libraries(test_mongo_connect mongo::mongocxx_shared)
foreach(FILE_I ${MONGO_CONCPP_DLLS})
    add_custom_command(
        TARGET test_mongo_connect 
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} 
        ARGS -E copy_if_different ${FILE_I} "${PROJECT_SOURCE_DIR}/build/Release"
    )
endforeach(FILE_I)

# data测试
add_executable(test_data 
                tests/test_data.cpp 
                utils/data.hpp
                utils/data.cpp
)

# 线程池测试
add_executable(test_thread_pool 
                tests/test_thread_pool.cpp 
                scheduler/thread_pool.cpp
                scheduler/thread_pool.cpp
)

# dag调度器测试
add_executable(test_dag
                tests/test_dag.cpp 
                scheduler/dag.cpp
                scheduler/dag.cpp
                scheduler/thread_pool.cpp
                scheduler/thread_pool.cpp
)



# 创建 docker run -itd --name lhrora11204 -h lhrora11204 --privileged=true -p 1521:1521 -p 222:22  -p 1158:1158 lhrbest/oracle_11g_ee_lhr_11.2.0.4:1.0 init
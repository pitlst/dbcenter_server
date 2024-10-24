#--------------------------------------------------------------------------------------#
#   项目总体设置
#--------------------------------------------------------------------------------------#
cmake_minimum_required(VERSION 3.12)
# 设置项目名称
set(PROJECT_NAME dbcenter_server)
# 在这里直接指定项目为c++项目，节约查找tool chain的时间
project(${PROJECT_NAME} VERSION 1.0.0 LANGUAGES CXX)
# 设置c++标准等级和编译后输出路径
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

#--------------------------------------------------------------------------------------#
#   项目依赖设置
#--------------------------------------------------------------------------------------#


# 设置头文件查找路径
include_directories(
    general
    connect
    logger
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
                general/base.hpp
                general/data.hpp
                general/data.cpp
                general/json.hpp
                general/json.cpp
                general/watch_dog.hpp
                general/watch_dog.cpp
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
                logger/logger.hpp
                logger/logger.cpp
                scheduler/dag.hpp
                scheduler/dag.cpp
                tasks/process_error.hpp
                tasks/process_error.cpp
                )

# target_link_libraries(${PROJECT_NAME} PRIVATE pthread ${OpenCV_LIBS} ${HIK_LIBS} ${Boost_LIBRARIES} openvino::runtime)
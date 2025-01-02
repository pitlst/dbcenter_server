#--------------------------------------------------------------------------------------#
#   项目总体设置
#--------------------------------------------------------------------------------------#
cmake_minimum_required(VERSION 3.12)
# 设置项目名称
set(PROJECT_NAME dbcenter_server)
# 在这里直接指定项目为c++项目，节约查找tool chain的时间
project(${PROJECT_NAME} VERSION 1.0.0 LANGUAGES CXX)
# 设置c++标准等级为17
set(CMAKE_CXX_STANDARD 20)
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
    3rd_party/nlohmann
    3rd_party/toml11
    utils
    tasks/visitor
    tasks/person
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

# intelTBB加速库
set(TBB_DIR "${PROJECT_SOURCE_DIR}/3rd_party/onetbb/lib/cmake/tbb")
set(TBB_DLLS
    ${TBB_DIR}/../../../redist/intel64/vc14/tbb12.dll
    ${TBB_DIR}/../../../redist/intel64/vc14/tbbbind_2_0.dll
    ${TBB_DIR}/../../../redist/intel64/vc14/tbbbind_2_5.dll
    ${TBB_DIR}/../../../redist/intel64/vc14/tbbbind.dll
    ${TBB_DIR}/../../../redist/intel64/vc14/tbbmalloc_proxy.dll
    ${TBB_DIR}/../../../redist/intel64/vc14/tbbmalloc.dll
)
find_package(TBB REQUIRED)

# fmt打印时格式化
set(fmt_DIR "${PROJECT_SOURCE_DIR}/3rd_party/fmt/lib/cmake/fmt")
find_package(fmt REQUIRED)

# 把项目目录定义成宏传递给程序
add_compile_definitions(PROJECT_PATH=\"${PROJECT_SOURCE_DIR}/\")
# 解决msvc的C4996警告
add_compile_definitions(_CRT_SECURE_NO_WARNINGS=1)
# 所有通用组件文件
set(ALL_UTILS_FILE                 
    utils/general.hpp
    utils/general.cpp
    utils/data.cpp
    utils/data.cpp
    utils/mongo.hpp
    utils/mongo.cpp
    utils/logger.hpp
    utils/logger.cpp
    utils/pipeline.cpp
    utils/pipeline.cpp
)

# 粘贴必要的动态链接库
foreach(FILE_I ${MONGO_CONCPP_DLLS})
    file(COPY ${FILE_I} DESTINATION "${PROJECT_SOURCE_DIR}/build/Release" FOLLOW_SYMLINK_CHAIN)
endforeach(FILE_I)
foreach(FILE_I ${TBB_DLLS})
    file(COPY ${FILE_I} DESTINATION "${PROJECT_SOURCE_DIR}/build/Release" FOLLOW_SYMLINK_CHAIN)
endforeach(FILE_I)

#--------------------------------------------------------------------------------------#
#   测试执行测试
#--------------------------------------------------------------------------------------#
# mongo链接测试
add_executable(test_mongo_connect tests/test_mongo_connect.cpp  ${ALL_UTILS_FILE})
target_link_libraries(test_mongo_connect mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)
# logger测试
add_executable(test_logger tests/test_logger.cpp  ${ALL_UTILS_FILE})
target_link_libraries(test_logger mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)
# data测试
add_executable(test_data tests/test_data.cpp  ${ALL_UTILS_FILE})
target_link_libraries(test_data mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)

#--------------------------------------------------------------------------------------#
#   项目编译设置
#--------------------------------------------------------------------------------------#

# 外网访客系统数据处理
set(VISITOR_DIRS "tasks/visitor")
add_executable(${PROJECT_NAME}_visitor ${VISITOR_DIRS}/visitor.cpp ${ALL_UTILS_FILE})
target_link_libraries(${PROJECT_NAME}_visitor mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)

# 短信数据处理
# 薪酬统计
set(MESSAGE_DIRS "tasks/message")
add_executable(${PROJECT_NAME}_message_format ${MESSAGE_DIRS}/format.cpp ${ALL_UTILS_FILE})
target_link_libraries(${PROJECT_NAME}_message_format mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)
# 薪酬求和
add_executable(${PROJECT_NAME}_message_sum ${MESSAGE_DIRS}/sum.cpp ${ALL_UTILS_FILE})
target_link_libraries(${PROJECT_NAME}_message_sum mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)

# 人员基础数据统计
set(PERSON_DIRS "tasks/person")
add_executable(${PROJECT_NAME}_person ${PERSON_DIRS}/person.cpp ${ALL_UTILS_FILE})
target_link_libraries(${PROJECT_NAME}_person mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)

# 业联系统数据处理
# 拼接
set(BC_DIRS "tasks/business_connection")
add_executable(${PROJECT_NAME}_bc_join ${BC_DIRS}/join.cpp ${ALL_UTILS_FILE})
target_link_libraries(${PROJECT_NAME}_bc_join mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)
# 处理
add_executable(${PROJECT_NAME}_bc_process ${BC_DIRS}/process.cpp ${ALL_UTILS_FILE})
target_link_libraries(${PROJECT_NAME}_bc_process mongo::mongocxx_shared TBB::tbb TBB::tbbmalloc fmt::fmt)








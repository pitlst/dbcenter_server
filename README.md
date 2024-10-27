# dbcenter_server 用于公司windows server的ETL服务

### 简述

本项目致力于高效率解决业务中需要的数据贯通和数据处理需求

目前的计划是使用python做异种数据库的同步，并实现一个基于数据量的同步间隔控制算法

也是使用python的streamlit库启动对应的网页服务，以实现敏捷开发与持续集成，
在这方面的改进是放弃文件定时导出中转的方式，转而在数据库中存放表格

对于数据处理脚本则使用c++，目前的前端计算服务需求和使用量过小，没有计划将后端服务转为c++
主要是定时处理和python与c++程序的信号量交互需要处理

目前的计划是c++程序单独在一个服务器上跑，而python程序和各大数据库在一个服务器上跑

因为python主要是io操作，计算较少，计划将10.24.5.32内网服务器在处理服务编写完成后从8u64g减少为4u16g
将更多的cpu资源分配给处理程序也就是c++，创建虚拟机时使用直通模式即可

考虑到后续的可维护性，c++系统还是使用Windows server 2019，工具链和ABI使用MSVC的

同时预备将以前所有的sql和python项目全部废弃，全部同步到最新的该项目上，所以实际上python还是有一个针对旧有的python处理流程的兼容层
但是该兼容层不准备做新的特性更新

在完成第一个正式版开发之前，旧有项目继续开发以维持业务迭代

### 环境准备

1. 安装mysql connect c++

默认识别路径为 3rd_party/mysql-concpp

使用官方的安装msi即可

2. 安装mongo c++

默认识别路径为 3rd_party/mongo-cxx

> curl -OL https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.10.1/mongo-cxx-driver-r3.10.1.tar.gz\
> tar -xzf mongo-cxx-driver-r3.10.1.tar.gz\
> cd mongo-cxx-driver-r3.10.1/build \
> 'C:\Program Files (x86)\CMake\bin\cmake.exe' .. -G "Visual Studio 15 2017" -A "x64"         -DCMAKE_CXX_STANDARD=17  -DCMAKE_INSTALL_PREFIX=C:\mongo-cxx-driver  \
>cmake --build . --config Release\
>cmake --build . --target install --config Release


### 编译部署

> mkdir build \
> cd build \
> cmake .. \
> cmake --build .

### 项目使用
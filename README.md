# dbcenter_server 用于公司windows server的ETL相关服务

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

### 开发环境准备

1.编译安装soci

> git clone https://github.com/nanodbc/nanodbc.git
> git checkout v2.14.0
> mkdir build
> cd build
> cmake -G "Visual Studio 17 2022" -A "x64" -DNANODBC_DISABLE_TESTS=TRUE -DNANODBC_ENABLE_UNICODE=TRUE -DNANODBC_DISABLE_EXAMPLES=TRUE ..
> cmake --build .


3. 安装mongo c++

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

### 项目结构与编写思路

项目主要分为三部分（处理、同步、web），三个数据库（mongodb、pgsql、mysql）

1. web部分

主要的用处是提供数据的下载接口和统计展示，因为经常变动且访问量极少，所以使用python的streamlit快速开发与部属

常用的基础部分就是数据库连接与日志，已经基本开发完成并准备上线部署，后续就是页面的计算逻辑与更新

2. 同步部分

使用python做异构数据库同步，由于公司内网的复杂性，无法做到实时触发更新，只能根据数据量定时触发更新

使用socket做ipc，在节点完成后通知处理部分进行后续的处理，使用线程池并行，主进程轮询触发同步

同步部分仅作数据的输入，不做数据的输出

数据的来源主要有三个类型：公司各大系统的数据库、内网文件系统和爬虫

3. 处理部分

这也是最复杂的部分，目前是准备使用c++做数据处理，因为需要根据要求定制化开发，所以这部分的变更也是最多的并且会持续更新。

持续集成由于公司的内网情况需要自行定义与编写，使用powershell脚本自动打包代码，使用python的爬虫自动填报上传内网，在内网也使用脚本自动解压，然后powershell定时自动增量编译打包部署，以实现公司特殊内外网环境下的持续集成



4. mysql

用于与外界系统交互，这里的数据都是删表重写的

5. mongo

用于日志，一些复杂数据结构，程序运行实时状态的存储，不对外开放

6. pgsql

用于同步的数据库，和mongo一样不对外开放，同步的数据缓存和处理的变更记录都在这里
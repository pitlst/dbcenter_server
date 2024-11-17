# dbcenter_server 用于公司windows server的ETL相关服务

### 简述

本项目致力于高效率解决业务中需要的数据贯通和数据处理需求

sync中使用python做数据抽取，方便异种数据集成和测试，数据仓库的数据库使用PostgreSQL 17

process中使用c++做数据处理，使用触发器启动数据处理流程

考虑到后续的可维护性，c++还是使用Windows server 2019，Visual Studio 2022， c++17

但是为了以后信创移植的便利性，应当在本地做好linux的移植测试

### 开发环境准备

1. 编译安装soci

默认识别路径为 process/3rd_party/soci

> git clone https://github.com/nanodbc/nanodbc.git
> git checkout v2.14.0
> mkdir build
> cd build
> cmake -G "Visual Studio 17 2022" -A x64 -DWITH_DB2=OFF -DWITH_FIREBIRD=OFF -DWITH_MYSQL=OFF -DWITH_ODBC=ON -DWITH_ORACLE=ON -DWITH_POSTGRESQL=ON -DWITH_SQLITE3=ON -DWITH_BOOST=OFF ..
> cmake --build . --config Release -j16
> cmake --install . --prefix project_path\process\3rd_party\soci

2. 下载toml11和json

这两个直接头文件集成即可

默认识别路径为 process/3rd_party/nlohmann 和 process/3rd_party/toml11

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
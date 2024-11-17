# dbcenter_server 用于公司windows server的ETL相关服务

### 简述

本项目致力于高效率解决业务中需要的数据贯通和数据处理需求

sync中使用python做数据抽取，方便异种数据集成和测试，数据仓库的数据库使用MongoDB

process中使用c++做数据处理，使用触发器启动数据处理流程

考虑到后续的可维护性，c++还是使用Windows server 2019，Visual Studio 2022， c++17

但是为了以后信创移植的便利性，应当在本地做好linux的移植测试

### 编译与部署流程

1. 安装mongo connect c++

默认识别路径为 process/3rd_party/mongo-cxx

```bash
curl -OL https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.10.1/mongo-cxx-driver-r3.10.1.tar.gz\
tar -xzf mongo-cxx-driver-r3.10.1.tar.gz
cd mongo-cxx-driver-r3.10.1/build 
'C:\Program Files (x86)\CMake\bin\cmake.exe' .. -G "Visual Studio 17 2022" -A "x64"  -DCMAKE_CXX_STANDARD=17  -DCMAKE_INSTALL_PREFIX=C:\\mongo-cxx-driver  
cmake --build . --config Release -j16
```

2. 下载toml11和json

这两个直接头文件集成即可

默认识别路径为 process/3rd_party/nlohmann 和 process/3rd_party/toml11

如何获取这两个库的纯头文件可以查看对应仓库的README

3. 开始编译项目

```bash
cd process
mkdir build 
cd build 
cmake .. -G "Visual Studio 17 2022" -A "x64"
cmake --build . --config Release -j16
```

4. 执行script_tools中的打包脚本，将编译产物进行打包

5. 复制到目标内网服务器中运行start.sh即可

### process部分本地测试开发流程

1. 首先，在联网的本地开发服务器上准备docker

2. 运行准备好的script_tools中的docker-compose.yml

### dag运行任务图的编写要求

主要要求就是节点的名称不能重复，不能是除了英文，数字和特殊字符的其他组合，别名无所谓

其用于向节点运行数据库输入和输出的节点不需要编写前向依赖，服务会自动按照数据量设置触发同步时间

对于所有的sql节点。建议是提前做好压缩，要么就处理成存储过程，放在sql中调用，总而言之计算尽可能不要放在数据服务上

注意从mongo到关系型数据库的映射需要mongo中的bson为特殊格式，不然无法做到输出，所以这部分的梳理需要在process中处理完再送到关系型数据库中

所有大计算量处理服务的节点均标为process，由c++处理，注意process节点仅支持从mongo到mongo

### 项目结构与编写思路

项目主要分为三部分（处理、同步、web），一个数据库（mongodb）

1. web部分

主要的用处是提供数据的下载接口和统计展示，因为经常变动且访问量极少，所以使用python的streamlit快速开发与部属

常用的基础部分就是数据库连接与日志，已经基本开发完成并准备上线部署，后续就是页面的计算逻辑与更新

2. 同步部分

使用python做异构数据库同步，由于公司内网的复杂性，无法做到非侵入式的实时触发更新，只能根据数据量定时轮询触发更新

3. 处理部分

这也是最复杂的部分，目前是准备使用c++做数据处理，因为需要根据要求定制化开发，所以这部分的变更也是最多的并且会持续更新

这一部分会专门准备测试环境方便外网开发

使用数据库触发器对

5. 本地机上的mongo

用于日志，复杂数据结构，同步中间表的存储，不对外开放


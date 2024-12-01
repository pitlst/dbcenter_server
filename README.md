# dbcenter_server 工作用ETL相关服务 

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

5. 复制到目标内网服务器中运行start.ps1即可

### 项目结构与编写思路

项目主要分为三部分（处理、同步、web），一个数据库（mongodb）

1. web部分

主要的用处是提供数据的下载接口和统计展示，因为经常变动且访问量极少，所以使用python的streamlit快速开发与部属

常用的基础部分就是数据库连接与日志，已经基本开发完成并准备上线部署，后续就是页面的计算逻辑与更新

后续准备转换成next.js的全套技术栈响应复杂需求（主要是BI看板和复杂交互式响应报表）

2. 本地机上的mongo

用于日志，消息队列，数据持久化，同步中间表的存储，不对外开放

3. 同步部分

使用python做异构数据库同步，从mongo中的消息队列中拿消息，接到信号后开始对应节点的同步

4. 处理部分

这也是最复杂的部分，目前是准备使用c++做数据处理，因为需要根据要求定制化开发，所以这部分的变更也是最多的并且会持续更新

同样是根据mongo消息队列拿消息做同步

这一部分会专门准备测试环境方便外网开发

5. 调度器

使用python实现

对于有依赖的节点正常按照dag节点的调度逻辑启动

对于没有前向依赖的节点复杂一些且需求与外界不同，需要按照其来源系统做间隔调度，具体则是对于连接同一个目标业务系统的节点，尽可能在时间范围内均衡调度，降低对目标系统的负载压力

目前的简单实现的想法是1个小时一次执行，对于同一个业务节点不做并发，仅在上一个节点完成后再进行下一个任务执行，允许时间超标，在上一个任务执行完之前对下一个任务排队，不同时并行多次发送

对于process节点和向非重点业务系统输出的节点则京可能提升吞吐量与并发，尤其process节点应当重点减少延时
写的一坨狗屎,准备单开重写了

尝试手写一个简易版mysql:
模块分为
- Transaction Manager
- Data Manager 
  - Log Manager
  - Page Manager
  - DataItem Manager
- Version Manager
  - Lock Table Manager
- Table Manager
- Index Manager
- SQL Parser 
- Server

接下来的计划

3. 添加索引系统
   优化查询性能：

实现B+树索引结构
添加索引管理器
支持主键和唯一索引
实现索引扫描和查找

4. 元数据管理
   构建数据字典：

实现表结构元数据存储
添加数据类型系统
支持约束（主键、外键、唯一性等）

5. SQL解析和执行层
   构建用户接口：

实现简单的SQL解析器
添加执行计划生成器
开发查询执行引擎
支持基本SQL操作（SELECT、INSERT、UPDATE、DELETE）

6. 查询优化
   提高查询性能：

实现基于成本的优化器
添加统计信息收集
实现查询重写规则
支持索引选择

7. 网络接口和客户端
   让数据库可用：

实现简单的网络协议
开发命令行客户端
添加基本的权限管理
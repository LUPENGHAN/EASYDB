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
1. 完善恢复系统
   首先应该完成系统恢复机制，确保数据库的ACID属性中的D（持久性）和A（原子性）：

完成RecoveryManager中的分析阶段、重做阶段和撤销阶段实现
增强检查点机制，实现增量检查点
添加崩溃恢复测试

2. 加强MVCC并发控制
   增强事务隔离能力：

实现完整的可见性规则
支持不同的隔离级别（读已提交、可重复读等）
添加锁管理器，支持乐观和悲观并发控制
实现死锁检测和处理

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
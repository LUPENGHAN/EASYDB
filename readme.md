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

[✅] TM 通过xid维护状态 active commit abort 

[✅] Log 通过undolog和redolog 进行记录和版本管理
Including:
1. 日志记录的写入 
   1. 提供日志记录的追加写入接口 
   2. 支持Redo Log和Undo Log两种日志类型 
   3. 确保日志写入的原子性和持久性
2. 日志文件管理 
   1. 创建和管理日志文件 
   2. 处理日志文件的轮转(当单个日志文件太大时)
   3. 提供日志文件的打开、读取和关闭功能
3. 检查点(Checkpoint)机制
   1. 定期创建检查点记录 
   2. 检查点应记录当前数据库状态的快照 
   3. 用于加速数据库恢复过程
4. 恢复功能 
   1. 系统崩溃后通过日志进行恢复 
   2. 按照事务提交顺序重做(Redo)已提交事务 
   3. 撤销(Undo)未提交事务的操作
5. WAL(Write-Ahead Logging)协议实现
   1. 确保数据修改前，相关日志已持久化到磁盘 
   2. 提供日志缓冲区的管理 
   3. 支持日志强制刷盘操作(Force Log)

未完待续
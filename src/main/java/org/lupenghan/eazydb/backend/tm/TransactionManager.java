package org.lupenghan.eazydb.backend.tm;

public interface TransactionManager {
    /*
    xid 是用来储存事务管理状态 (xidCounter)
    0: 活动状态（ACTIVE）
    1: 已提交（COMMITTED）
    2: 已中止（ABORTED
    事务区域：每个事务占用1字节，表示事务的状态
     */
    //开始一个新的事务
    long begin();
    //提交一个事务
    void commit(long xid);
    //终止一个事务
    void abort(long xid);
    //回滚
//    void rollback(long xid);
    //查询事务的进行状态
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAnort(long xid);


//    boolean isRollbackRequired(long xid);


    //关闭事务管理器
    void close(long xid);

    // 创建事务管理器
    public static TransactionManager create(String path);

    // 打开已存在的事务管理器
    public static TransactionManager open(String path);

}

package org.lupenghan.eazydb.backend.TransactionManager;

import java.util.Set;

/**
 * 事务管理器接口的MVCC扩展
 */
public interface MVCCTransactionManager extends TransactionManager {
    /**
     * 获取事务开始时间戳
     * @param xid 事务ID
     * @return 开始时间戳
     */
    long getBeginTimestamp(long xid);

    /**
     * 获取事务提交时间戳
     * @param xid 事务ID
     * @return 提交时间戳
     */
    long getCommitTimestamp(long xid);

    /**
     * 获取当前活跃的事务集合
     * @return 活跃事务集合
     */
    Set<Long> getActiveTransactions();

    /**
     * 为事务创建ReadView
     * @param xid 事务ID
     * @return ReadView对象
     */
    ReadView createReadView(long xid);

    /**
     * 设置事务的隔离级别
     * @param xid 事务ID
     * @param level 隔离级别
     */
    void setIsolationLevel(long xid, IsolationLevel level);

    /**
     * 获取事务的隔离级别
     * @param xid 事务ID
     * @return 隔离级别
     */
    IsolationLevel getIsolationLevel(long xid);
}
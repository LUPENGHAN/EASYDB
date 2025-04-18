package org.lupenghan.eazydb.backend.TransactionManager;

import org.lupenghan.eazydb.backend.TransactionManager.utils.IsolationLevel;
import org.lupenghan.eazydb.backend.TransactionManager.utils.ReadView;

import java.util.Set;

/**
 * 事务管理器接口，整合基本事务管理、MVCC和锁管理功能
 */
public interface TransactionManager {
    // ---- 基本事务管理功能 ----

    /**
     * 开始一个新的事务
     * @return 事务ID
     */
    long begin();

    /**
     * 提交一个事务
     * @param xid 事务ID
     */
    void commit(long xid);

    /**
     * 终止一个事务
     * @param xid 事务ID
     */
    void abort(long xid);

    /**
     * 查询事务的进行状态
     */
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAbort(long xid);

    /**
     * 关闭事务管理器
     */
    void close();

    /**
     * 创建检查点
     */
    void checkpoint();

    // ---- MVCC功能 ----

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

    // ---- 锁管理功能 ----

    /**
     * 设置锁请求超时
     * @param timeout 超时时间（毫秒）
     */
    void setLockTimeout(long timeout);

    /**
     * 获取共享锁（读锁）
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功获取锁
     */
    boolean acquireSharedLock(long xid, Object resourceID);

    /**
     * 获取排他锁（写锁）
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功获取锁
     */
    boolean acquireExclusiveLock(long xid, Object resourceID);

    /**
     * 释放锁
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功释放锁
     */
    boolean releaseLock(long xid, Object resourceID);

    /**
     * 检测死锁
     * @return 检测到的死锁中的一个受害者事务ID，如果没有死锁则返回0
     */
    long detectDeadlock();
}
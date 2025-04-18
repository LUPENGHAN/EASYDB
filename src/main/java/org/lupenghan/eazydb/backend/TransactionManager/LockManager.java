package org.lupenghan.eazydb.backend.TransactionManager;

import org.lupenghan.eazydb.backend.TransactionManager.utils.DeadlockException;

/**
 * 锁管理器接口
 * 负责管理事务的锁请求、授予和释放
 */
public interface LockManager {
    /**
     * 锁类型枚举
     */
    enum LockType {
        SHARED,       // 共享锁（读锁）
        EXCLUSIVE     // 排他锁（写锁）
    }

    /**
     * 请求锁
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @param lockType 锁类型
     * @param timeout 超时时间（毫秒），0表示立即返回，-1表示无限等待
     * @return 是否成功获取锁
     * @throws DeadlockException 如果检测到死锁
     */
    boolean acquireLock(long xid, Object resourceID, LockType lockType, long timeout) throws DeadlockException;

    /**
     * 释放事务持有的指定资源的锁
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功释放锁
     */
    boolean releaseLock(long xid, Object resourceID);

    /**
     * 释放事务持有的所有锁
     * @param xid 事务ID
     */
    void releaseAllLocks(long xid);

    /**
     * 检查事务是否持有指定资源的锁
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @param lockType 锁类型
     * @return 是否持有锁
     */
    boolean holdsLock(long xid, Object resourceID, LockType lockType);

    /**
     * 检测死锁
     * @return 检测到的死锁中的一个受害者事务ID，如果没有死锁则返回0
     */
    long detectDeadlock();
}
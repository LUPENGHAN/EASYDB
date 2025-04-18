package org.lupenghan.eazydb.backend.TransactionManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 增强版事务管理器，集成了锁管理器
 */
public class EnhancedTransactionManagerImpl extends MVCCTransactionManagerImpl {
    // 锁管理器
    private final LockManager lockManager;

    // 事务锁请求超时（毫秒），-1表示无限等待
    private long lockTimeout = 10000;

    /**
     * 创建增强版事务管理器
     * @param path 数据库路径
     * @throws Exception 如果创建失败
     */
    public EnhancedTransactionManagerImpl(String path) throws Exception {
        super(path);
        this.lockManager = new LockManagerImpl();
    }

    /**
     * 设置锁请求超时
     * @param timeout 超时时间（毫秒）
     */
    public void setLockTimeout(long timeout) {
        this.lockTimeout = timeout;
    }

    /**
     * 获取锁管理器
     * @return 锁管理器
     */
    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * 获取共享锁（读锁）
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功获取锁
     */
    public boolean acquireSharedLock(long xid, Object resourceID) {
        // 根据隔离级别决定是否获取锁
        IsolationLevel level = getIsolationLevel(xid);

        // READ_UNCOMMITTED不需要读锁
        if (level == IsolationLevel.READ_UNCOMMITTED) {
            return true;
        }

        // SERIALIZABLE需要读锁
        if (level == IsolationLevel.SERIALIZABLE) {
            try {
                return lockManager.acquireLock(xid, resourceID, LockManager.LockType.SHARED, lockTimeout);
            } catch (DeadlockException e) {
                // 发生死锁，当前事务被选为受害者
                abort(xid);
                return false;
            }
        }

        // 其他隔离级别依赖MVCC，不需要读锁
        return true;
    }

    /**
     * 获取排他锁（写锁）
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功获取锁
     */
    public boolean acquireExclusiveLock(long xid, Object resourceID) {
        // 所有隔离级别的写操作都需要排他锁
        try {
            return lockManager.acquireLock(xid, resourceID, LockManager.LockType.EXCLUSIVE, lockTimeout);
        } catch (DeadlockException e) {
            // 发生死锁，当前事务被选为受害者
            abort(xid);
            return false;
        }
    }

    /**
     * 释放锁
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功释放锁
     */
    public boolean releaseLock(long xid, Object resourceID) {
        return lockManager.releaseLock(xid, resourceID);
    }

    @Override
    public void commit(long xid) {
        // 先获取提交时间戳
        long commitTS = TimestampGenerator.nextTimestamp();

        // 调用父类方法更新事务状态
        super.commit(xid);

        // 释放该事务持有的所有锁
        lockManager.releaseAllLocks(xid);
    }

    @Override
    public void abort(long xid) {
        // 调用父类方法更新事务状态
        super.abort(xid);

        // 释放该事务持有的所有锁
        lockManager.releaseAllLocks(xid);
    }
}
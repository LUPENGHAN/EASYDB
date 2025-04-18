package org.lupenghan.eazydb.backend.TransactionManager;

import org.lupenghan.eazydb.backend.TransactionManager.Impl.TransactionManagerImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 混合并发控制策略管理器
 * 根据操作特点动态选择乐观或悲观并发控制
 */
public class ConcurrencyControlManager {
    // 事务管理器
    private final TransactionManager txManager;

    // 访问计数阈值，超过此阈值将切换为悲观控制
    private final int conflictThreshold = 5;

    // 资源访问统计：资源ID -> 访问次数
    private final Map<Object, Integer> accessCounts = new ConcurrentHashMap<>();

    // 资源冲突标记：资源ID -> 是否使用悲观控制
    private final Map<Object, Boolean> pessimisticResources = new ConcurrentHashMap<>();

    /**
     * 创建并发控制管理器
     * @param txManager 事务管理器
     */
    public ConcurrencyControlManager(TransactionManagerImpl txManager) {
        this.txManager = txManager;
    }

    /**
     * 记录资源访问
     * @param resourceID 资源ID
     */
    public void recordAccess(Object resourceID) {
        int count = accessCounts.getOrDefault(resourceID, 0) + 1;
        accessCounts.put(resourceID, count);

        // 如果访问次数超过阈值，标记为悲观控制
        if (count > conflictThreshold) {
            pessimisticResources.put(resourceID, true);
        }
    }

    /**
     * 记录冲突
     * @param resourceID 资源ID
     */
    public void recordConflict(Object resourceID) {
        // 直接标记为悲观控制
        pessimisticResources.put(resourceID, true);
    }

    /**
     * 判断是否应该使用悲观并发控制
     * @param resourceID 资源ID
     * @return 是否使用悲观控制
     */
    public boolean shouldUsePessimisticControl(Object resourceID) {
        return pessimisticResources.getOrDefault(resourceID, false);
    }

    /**
     * 获取读锁（根据策略决定是否实际获取）
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功（乐观模式总是返回true）
     */
    public boolean acquireReadLock(long xid, Object resourceID) {
        recordAccess(resourceID);

        if (shouldUsePessimisticControl(resourceID)) {
            // 使用悲观控制，获取实际锁
            return txManager.acquireSharedLock(xid, resourceID);
        } else {
            // 使用乐观控制，不获取实际锁
            return true;
        }
    }

    /**
     * 获取写锁（根据策略决定是否实际获取）
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @return 是否成功
     */
    public boolean acquireWriteLock(long xid, Object resourceID) {
        recordAccess(resourceID);

        if (shouldUsePessimisticControl(resourceID)) {
            // 使用悲观控制，获取实际锁
            return txManager.acquireExclusiveLock(xid, resourceID);
        } else {
            // 使用乐观控制，标记写意图但不获取实际锁
            // 在提交时进行冲突检测
            return true;
        }
    }

    /**
     * 执行乐观冲突检测
     * @param xid 事务ID
     * @param resourceID 资源ID
     * @param expectedVersion 期望的版本
     * @param actualVersion 实际版本
     * @return 是否有冲突
     */
    public boolean checkOptimisticConflict(long xid, Object resourceID, long expectedVersion, long actualVersion) {
        if (expectedVersion != actualVersion) {
            // 检测到冲突，记录并可能切换策略
            recordConflict(resourceID);
            return true;
        }
        return false;
    }

    /**
     * 清理事务相关的并发控制信息
     * @param xid 事务ID
     */
    public void cleanupTransaction(long xid) {
        // 在实际实现中，这里需要清理乐观控制模式下的事务写意图等
    }
}
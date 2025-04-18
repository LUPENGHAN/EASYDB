package org.lupenghan.eazydb.backend.TransactionManager.Impl;

import lombok.Getter;
import org.lupenghan.eazydb.backend.TransactionManager.utils.DeadlockException;
import org.lupenghan.eazydb.backend.TransactionManager.LockManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 锁管理器实现
 */
public class LockManagerImpl implements LockManager {
    // 资源锁表：资源ID -> 锁信息
    private final Map<Object, LockEntry> lockTable;

    // 事务持有的锁：事务ID -> [资源ID集合]
    private final Map<Long, Set<Object>> txLocks;

    // 事务等待图：事务ID -> [等待的事务ID集合]
    private final Map<Long, Set<Long>> waitForGraph;

    // 全局锁，用于并发控制
    private final Lock globalLock;

    // 死锁检测间隔（毫秒）
    private static final long DEADLOCK_DETECTION_INTERVAL = 1000;

    // 上次死锁检测时间
    private long lastDeadlockCheck;

    /**
     * 创建锁管理器
     */
    public LockManagerImpl() {
        this.lockTable = new ConcurrentHashMap<>();
        this.txLocks = new ConcurrentHashMap<>();
        this.waitForGraph = new ConcurrentHashMap<>();
        this.globalLock = new ReentrantLock();
        this.lastDeadlockCheck = System.currentTimeMillis();
    }

    @Override
    public boolean acquireLock(long xid, Object resourceID, LockType lockType, long timeout) throws DeadlockException {
        long startTime = System.currentTimeMillis();
        boolean acquired = false;

        do {
            globalLock.lock();
            try {
                // 检查是否应该进行死锁检测
                checkDeadlock();

                // 获取或创建资源锁条目
                LockEntry lockEntry = lockTable.computeIfAbsent(resourceID, k -> new LockEntry());

                // 检查锁兼容性，如果兼容则直接授予锁
                if (isLockCompatible(lockEntry, xid, lockType)) {
                    grantLock(lockEntry, xid, lockType);
                    updateTxLocks(xid, resourceID);
                    acquired = true;
                    break;
                }

                // 锁不兼容，检查是否需要等待
                if (timeout == 0) {
                    // 不等待，直接返回
                    break;
                }

                // 创建锁请求并加入等待队列
                LockRequest request = new LockRequest(xid, lockType);
                lockEntry.waitQueue.add(request);

                // 更新等待图
                updateWaitForGraph(xid, lockEntry);

                // 检查死锁
                long victimXID = detectDeadlock();
                if (victimXID == xid) {
                    // 当前事务被选为死锁受害者
                    lockEntry.waitQueue.remove(request);
                    cleanupWaitForGraph(xid);
                    throw new DeadlockException("检测到死锁，事务 " + xid + " 被选为牺牲者", xid);
                } else if (victimXID > 0) {
                    // 另一个事务被选为死锁受害者，继续尝试获取锁
                    abortTransaction(victimXID);
                }

            } finally {
                globalLock.unlock();
            }

            // 检查超时
            if (timeout > 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= timeout) {
                    // 超时，移除请求并返回失败
                    globalLock.lock();
                    try {
                        LockEntry lockEntry = lockTable.get(resourceID);
                        if (lockEntry != null) {
                            lockEntry.waitQueue.removeIf(req -> req.xid == xid);
                        }
                        cleanupWaitForGraph(xid);
                    } finally {
                        globalLock.unlock();
                    }
                    break;
                }

                // 短暂等待然后重试
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.min(100, timeout - elapsed));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // 再次检查，可能在等待期间锁已经被授予
            globalLock.lock();
            try {
                LockEntry lockEntry = lockTable.get(resourceID);
                if (lockEntry != null && lockEntry.isHeldBy(xid, lockType)) {
                    acquired = true;
                    break;
                }
            } finally {
                globalLock.unlock();
            }

        } while (timeout != 0 && (timeout < 0 || System.currentTimeMillis() - startTime < timeout));

        return acquired;
    }

    /**
     * 检查是否应该进行死锁检测
     */
    private void checkDeadlock() {
        long now = System.currentTimeMillis();
        if (now - lastDeadlockCheck >= DEADLOCK_DETECTION_INTERVAL) {
            detectDeadlock();
            lastDeadlockCheck = now;
        }
    }

    /**
     * 判断锁是否兼容
     */
    private boolean isLockCompatible(LockEntry lockEntry, long xid, LockType lockType) {
        // 事务已持有该锁
        if (lockEntry.isHeldBy(xid, lockType)) {
            return true;
        }

        // 事务持有共享锁，现在想升级为排他锁
        if (lockType == LockType.EXCLUSIVE && lockEntry.holders.containsKey(xid)) {
            // 如果当前事务是唯一的锁持有者，可以升级
            return lockEntry.holders.size() == 1 && lockEntry.holders.containsKey(xid);
        }

        // 其他事务持有锁
        if (!lockEntry.holders.isEmpty()) {
            if (lockType == LockType.EXCLUSIVE) {
                // 排他锁要求没有其他持有者
                return false;
            } else {
                // 共享锁要求没有排他锁持有者
                return lockEntry.holders.values().stream().noneMatch(type -> type == LockType.EXCLUSIVE);
            }
        }

        // 没有锁持有者，可以授予
        return true;
    }

    /**
     * 授予锁
     */
    private void grantLock(LockEntry lockEntry, long xid, LockType lockType) {
        lockEntry.holders.put(xid, lockType);

        // 如果是等待队列中的请求，将其移除
        lockEntry.waitQueue.removeIf(req -> req.xid == xid);
    }

    /**
     * 更新事务持有的锁集合
     */
    private void updateTxLocks(long xid, Object resourceID) {
        txLocks.computeIfAbsent(xid, k -> new HashSet<>()).add(resourceID);
    }

    /**
     * 更新等待图
     */
    private void updateWaitForGraph(long waiterXID, LockEntry lockEntry) {
        // 事务waiterXID等待锁持有者集合
        Set<Long> waitFor = waitForGraph.computeIfAbsent(waiterXID, k -> new HashSet<>());
        waitFor.addAll(lockEntry.holders.keySet());
    }

    /**
     * 清理等待图中的事务
     */
    private void cleanupWaitForGraph(long xid) {
        // 移除该事务的等待记录
        waitForGraph.remove(xid);

        // 移除其他事务对该事务的等待
        for (Set<Long> waiters : waitForGraph.values()) {
            waiters.remove(xid);
        }
    }

    /**
     * 中止事务（由死锁检测触发）
     */
    private void abortTransaction(long xid) {
        globalLock.lock();
        try {
            // 释放该事务持有的所有锁
            releaseAllLocks(xid);

            // 通知事务管理器中止事务
            // 注意：在实际实现中，需要与事务管理器集成
            // txManager.abort(xid);

            // 清理等待图
            cleanupWaitForGraph(xid);
        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public boolean releaseLock(long xid, Object resourceID) {
        globalLock.lock();
        try {
            LockEntry lockEntry = lockTable.get(resourceID);
            if (lockEntry == null || !lockEntry.holders.containsKey(xid)) {
                return false;
            }

            // 移除锁持有者
            lockEntry.holders.remove(xid);

            // 更新事务持有的锁集合
            Set<Object> resources = txLocks.get(xid);
            if (resources != null) {
                resources.remove(resourceID);
                if (resources.isEmpty()) {
                    txLocks.remove(xid);
                }
            }

            // 尝试授予等待的锁
            processWaitQueue(lockEntry);

            // 如果没有锁持有者和等待者，移除锁条目
            if (lockEntry.holders.isEmpty() && lockEntry.waitQueue.isEmpty()) {
                lockTable.remove(resourceID);
            }

            return true;
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 处理等待队列，尝试授予锁
     */
    private void processWaitQueue(LockEntry lockEntry) {
        // 使用队列副本，避免并发修改异常
        Queue<LockRequest> waitQueue = new LinkedList<>(lockEntry.waitQueue);

        // 尝试为等待队列中的请求授予锁
        for (LockRequest request : waitQueue) {
            if (isLockCompatible(lockEntry, request.xid, request.lockType)) {
                grantLock(lockEntry, request.xid, request.lockType);
                updateTxLocks(request.xid, lockEntry);

                // 更新等待图
                cleanupWaitForGraph(request.xid);
            } else {
                // 如果不能授予锁，停止处理（保持FIFO顺序）
                break;
            }
        }
    }

    @Override
    public void releaseAllLocks(long xid) {
        globalLock.lock();
        try {
            // 获取事务持有的所有资源
            Set<Object> resources = txLocks.get(xid);
            if (resources == null) {
                return;
            }

            // 释放每个资源的锁
            for (Object resourceID : new HashSet<>(resources)) {
                releaseLock(xid, resourceID);
            }

            // 清理事务锁记录
            txLocks.remove(xid);

            // 清理等待图
            cleanupWaitForGraph(xid);
        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public boolean holdsLock(long xid, Object resourceID, LockType lockType) {
        globalLock.lock();
        try {
            LockEntry lockEntry = lockTable.get(resourceID);
            if (lockEntry == null) {
                return false;
            }
            return lockEntry.isHeldBy(xid, lockType);
        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public long detectDeadlock() {
        globalLock.lock();
        try {
            // 使用DFS检测等待图中的环
            Set<Long> visited = new HashSet<>();
            Set<Long> currentPath = new HashSet<>();

            // 遍历所有节点
            for (Long xid : waitForGraph.keySet()) {
                if (!visited.contains(xid)) {
                    Long victim = dfsDetectCycle(xid, visited, currentPath);
                    if (victim > 0) {
                        return victim;
                    }
                }
            }

            return 0; // 没有检测到死锁
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 使用DFS检测环并选择受害者
     */
    private Long dfsDetectCycle(Long xid, Set<Long> visited, Set<Long> currentPath) {
        visited.add(xid);
        currentPath.add(xid);

        // 遍历该事务等待的所有事务
        Set<Long> waitFor = waitForGraph.get(xid);
        if (waitFor != null) {
            for (Long targetXid : waitFor) {
                // 如果在当前路径中发现已访问节点，则找到环
                if (currentPath.contains(targetXid)) {
                    // 选择环中事务ID最大的作为受害者（简单策略）
                    Long victim = xid;
                    for (Long txInCycle : currentPath) {
                        if (txInCycle > victim) {
                            victim = txInCycle;
                        }
                    }
                    return victim;
                }

                // 继续DFS
                if (!visited.contains(targetXid)) {
                    Long victim = dfsDetectCycle(targetXid, visited, currentPath);
                    if (victim > 0) {
                        return victim;
                    }
                }
            }
        }

        // 回溯
        currentPath.remove(xid);
        return 0L;
    }

    /**
     * 锁条目，表示一个资源上的锁状态
     */
    @Getter
    private static class LockEntry {
        // 锁持有者：事务ID -> 锁类型
        Map<Long, LockType> holders = new HashMap<>();

        // 等待队列
        Queue<LockRequest> waitQueue = new LinkedList<>();

        /**
         * 检查指定事务是否持有指定类型的锁
         */
        boolean isHeldBy(long xid, LockType lockType) {
            LockType heldType = holders.get(xid);
            if (heldType == null) {
                return false;
            }

            // 持有排他锁则满足任何锁请求
            if (heldType == LockType.EXCLUSIVE) {
                return true;
            }

            // 持有共享锁只满足共享锁请求
            return lockType == LockType.SHARED;
        }
    }

    /**
     * 锁请求
     */
    @Getter
    private static class LockRequest {
        final long xid;
        final LockType lockType;

        LockRequest(long xid, LockType lockType) {
            this.xid = xid;
            this.lockType = lockType;
        }
    }
}
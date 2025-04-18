package org.lupenghan.eazydb.backend.TransactionManager.Impl;

import org.lupenghan.eazydb.backend.TransactionManager.*;
import org.lupenghan.eazydb.backend.TransactionManager.utils.DeadlockException;
import org.lupenghan.eazydb.backend.TransactionManager.utils.IsolationLevel;
import org.lupenghan.eazydb.backend.TransactionManager.utils.ReadView;
import org.lupenghan.eazydb.backend.TransactionManager.utils.TimestampGenerator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 整合事务管理器实现，包含基本事务管理、MVCC和锁管理功能
 */
public class TransactionManagerImpl implements TransactionManager {
    // ---- 基本事务管理相关字段 ----

    // 固定数据
    private static final byte ACTIVE = 1;
    private static final byte COMMITTED = 2;
    private static final byte ABORT = 3;

    // 事务文件头大小（8字节存储XID计数器）
    private static final int XID_HEADER_LENGTH = 8;
    // 每个事务状态占用的字节数
    private static final int XID_FIELD_SIZE = 1;

    // 事务文件
    private RandomAccessFile xidFile;
    // 当前最大事务ID
    private long xidCounter;
    // 事务文件对应的通道
    private FileChannel fc;

    // 缓存已提交事务的集合
    private Set<Long> cachedCommittedTransactions;

    // ---- MVCC相关字段 ----

    // MVCC元数据文件相关
    private static final int ISOLATION_LEVEL_SIZE = 4;
    private static final int TIMESTAMP_SIZE = 8;
    private static final int TX_META_HEADER_SIZE = 8;

    // 元数据文件
    private RandomAccessFile metaFile;
    private FileChannel metaFC;

    // MVCC缓存
    private final Map<Long, Long> beginTimestamps;
    private final Map<Long, Long> commitTimestamps;
    private final Map<Long, IsolationLevel> isolationLevels;
    private final Map<Long, ReadView> readViews;

    // ---- 锁管理相关字段 ----

    // 锁超时时间（毫秒），-1表示无限等待
    private long lockTimeout = 10000;

    // 资源锁表：资源ID -> 锁信息
    private final Map<Object, LockEntry> lockTable;

    // 事务持有的锁：事务ID -> [资源ID集合]
    private final Map<Long, Set<Object>> txLocks;

    // 事务等待图：事务ID -> [等待的事务ID集合]
    private final Map<Long, Set<Long>> waitForGraph;

    // 全局锁
    private final ReentrantLock globalLock = new ReentrantLock();

    // 死锁检测间隔（毫秒）
    private static final long DEADLOCK_DETECTION_INTERVAL = 1000;

    // 上次死锁检测时间
    private long lastDeadlockCheck;

    /**
     * 创建事务管理器
     * @param path 数据库路径
     * @throws Exception 如果创建失败
     */
    public TransactionManagerImpl(String path) throws Exception {
        // ---- 初始化基本事务管理 ----

        File file = new File(path + "/xid.txn");
        boolean isNewXidFile = !file.exists();

        File metaFile = new File(path + "/txmeta.dat");
        boolean isNewMetaFile = !metaFile.exists();

        // 确保目录存在
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // 初始化事务状态文件
        xidFile = new RandomAccessFile(file, "rw");
        fc = xidFile.getChannel();

        if (isNewXidFile) {
            // 新文件，初始化XID为1（0是保留的无效事务ID）
            ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
            buffer.putLong(1);
            buffer.flip();
            fc.write(buffer, 0);
            xidCounter = 1;
        } else {
            // 已有文件，读取现有XID计数器
            ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
            fc.read(buf, 0);
            buf.flip();
            xidCounter = buf.getLong();
        }

        cachedCommittedTransactions = new HashSet<>();
        if (!isNewXidFile) {
            recoverCommittedTransactions();
        }

        // ---- 初始化MVCC ----

        // 初始化元数据文件
        this.metaFile = new RandomAccessFile(metaFile, "rw");
        this.metaFC = this.metaFile.getChannel();

        if (isNewMetaFile) {
            // 初始化元数据文件头
            ByteBuffer buffer = ByteBuffer.wrap(new byte[TX_META_HEADER_SIZE]);
            buffer.putLong(0); // 预留
            buffer.flip();
            metaFC.write(buffer, 0);
        }

        // 初始化MVCC缓存
        beginTimestamps = new ConcurrentHashMap<>();
        commitTimestamps = new ConcurrentHashMap<>();
        isolationLevels = new ConcurrentHashMap<>();
        readViews = new ConcurrentHashMap<>();

        if (!isNewMetaFile) {
            recoverTransactionMetadata();
        }

        // ---- 初始化锁管理 ----

        lockTable = new ConcurrentHashMap<>();
        txLocks = new ConcurrentHashMap<>();
        waitForGraph = new ConcurrentHashMap<>();
        lastDeadlockCheck = System.currentTimeMillis();
    }

    // ---- 基本事务管理方法 ----

    @Override
    public long begin() {
        long xid = xidCounter++;
        updateXID(xid, ACTIVE);

        // 分配时间戳
        long timestamp = TimestampGenerator.nextTimestamp();
        beginTimestamps.put(xid, timestamp);

        // 设置默认隔离级别为可重复读
        isolationLevels.put(xid, IsolationLevel.REPEATABLE_READ);

        // 更新XID计数器到文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
        buf.putLong(xidCounter);
        buf.flip();
        try {
            fc.write(buf, 0);
            fc.force(false); // 确保持久化

            // 保存事务元数据
            saveTransactionMetadata(xid, timestamp, IsolationLevel.REPEATABLE_READ);
        } catch (IOException e) {
            throw new RuntimeException("Failed to begin transaction", e);
        }

        return xid;
    }

    @Override
    public void commit(long xid) {
        if (!isActive(xid)) {
            return;
        }

        // 获取提交时间戳
        long commitTS = TimestampGenerator.nextTimestamp();
        commitTimestamps.put(xid, commitTS);

        // 更新事务状态
        updateXID(xid, COMMITTED);
        cachedCommittedTransactions.add(xid);

        // 清理不再需要的ReadView
        readViews.remove(xid);

        // 释放该事务持有的所有锁
        releaseAllLocks(xid);

        try {
            // 保存提交时间戳
            saveCommitTimestamp(xid, commitTS);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save commit timestamp", e);
        }
    }

    @Override
    public void abort(long xid) {
        if (!isActive(xid)) {
            return;
        }

        updateXID(xid, ABORT);

        // 清理事务相关的缓存
        beginTimestamps.remove(xid);
        commitTimestamps.remove(xid);
        isolationLevels.remove(xid);
        readViews.remove(xid);

        // 释放该事务持有的所有锁
        releaseAllLocks(xid);
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == 0) return false;
        return checkXIDState(xid, ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == 0) return true; // XID 0视为已提交

        // 先查缓存，提高性能
        if (cachedCommittedTransactions.contains(xid)) {
            return true;
        }

        return checkXIDState(xid, COMMITTED);
    }

    @Override
    public boolean isAbort(long xid) {
        if (xid == 0) return false; // XID 0不会是中止状态
        return checkXIDState(xid, ABORT);
    }

    @Override
    public void checkpoint() {
        try {
            // 强制将所有更改写入磁盘
            fc.force(true);
            if (metaFC != null) {
                metaFC.force(true);
            }

            // 在更复杂的实现中，可能需要协调其他管理器（如日志管理器）
        } catch (IOException e) {
            throw new RuntimeException("Failed to create checkpoint", e);
        }
    }

    @Override
    public void close() {
        try {
            xidFile.close();
            fc.close();

            if (metaFile != null) {
                metaFile.close();
            }
            if (metaFC != null) {
                metaFC.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close transaction files", e);
        }
    }

    // ---- MVCC相关方法 ----

    @Override
    public long getBeginTimestamp(long xid) {
        return beginTimestamps.getOrDefault(xid, 0L);
    }

    @Override
    public long getCommitTimestamp(long xid) {
        return commitTimestamps.getOrDefault(xid, 0L);
    }

    @Override
    public Set<Long> getActiveTransactions() {
        Set<Long> activeXids = new HashSet<>();

        // 检查系统中的所有事务
        for (long xid = 1; xid < xidCounter; xid++) {
            if (isActive(xid)) {
                activeXids.add(xid);
            }
        }

        return activeXids;
    }

    @Override
    public ReadView createReadView(long xid) {
        // 获取事务隔离级别
        IsolationLevel level = getIsolationLevel(xid);

        // 对于可重复读隔离级别，如果已经有ReadView则复用
        if (level == IsolationLevel.REPEATABLE_READ && readViews.containsKey(xid)) {
            return readViews.get(xid);
        }

        // 创建新的ReadView
        Set<Long> activeXids = getActiveTransactions();
        long readTS = TimestampGenerator.nextTimestamp();
        ReadView readView = new ReadView(xid, readTS, activeXids, level);

        // 对于可重复读隔离级别，缓存ReadView
        if (level == IsolationLevel.REPEATABLE_READ) {
            readViews.put(xid, readView);
        }

        return readView;
    }

    @Override
    public void setIsolationLevel(long xid, IsolationLevel level) {
        isolationLevels.put(xid, level);

        // 清除现有的ReadView，以便下次根据新的隔离级别创建
        readViews.remove(xid);

        try {
            // 保存隔离级别
            saveIsolationLevel(xid, level);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save isolation level", e);
        }
    }

    @Override
    public IsolationLevel getIsolationLevel(long xid) {
        return isolationLevels.getOrDefault(xid, IsolationLevel.REPEATABLE_READ);
    }

    // ---- 锁管理相关方法 ----

    @Override
    public void setLockTimeout(long timeout) {
        this.lockTimeout = timeout;
    }

    @Override
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
                return acquireLock(xid, resourceID, LockType.SHARED, lockTimeout);
            } catch (DeadlockException e) {
                // 发生死锁，当前事务被选为受害者
                abort(xid);
                return false;
            }
        }

        // 其他隔离级别依赖MVCC，不需要读锁
        return true;
    }

    @Override
    public boolean acquireExclusiveLock(long xid, Object resourceID) {
        // 所有隔离级别的写操作都需要排他锁
        try {
            return acquireLock(xid, resourceID, LockType.EXCLUSIVE, lockTimeout);
        } catch (DeadlockException e) {
            // 发生死锁，当前事务被选为受害者
            abort(xid);
            return false;
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
            processWaitQueue(lockEntry, resourceID);

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
     * 释放事务的所有锁
     * @param xid 事务ID
     */
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

    // ---- 辅助方法 - 基本事务管理 ----

    private void updateXID(long xid, byte status) {
        long offset = XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        buffer.put(status);
        buffer.flip();
        try {
            fc.write(buffer, offset);
            fc.force(false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update XID(transaction state)", e);
        }
    }

    private void recoverCommittedTransactions() throws IOException {
        long fileSize = fc.size();
        long xidCount = (fileSize - XID_HEADER_LENGTH) / XID_FIELD_SIZE;

        // 批量读取状态提高性能
        ByteBuffer buffer = ByteBuffer.allocate((int)fileSize - XID_HEADER_LENGTH);
        fc.read(buffer, XID_HEADER_LENGTH);
        buffer.flip();
        for (long i = 0; i < xidCount; i++) {
            byte status = buffer.get();
            if (status == COMMITTED) {
                cachedCommittedTransactions.add(i + 1);
            }
        }
    }

    private boolean checkXIDState(long xid, byte state) {
        long offset = XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;

        // 如果事务ID超出文件范围，则状态一定是活跃的
        try {
            if (offset >= fc.size()) {
                return state == ACTIVE;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check file size", e);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        try {
            fc.read(buf, offset);
            buf.flip();
            return buf.get() == state;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read transaction state", e);
        }
    }

    // ---- 辅助方法 - MVCC ----

    private void recoverTransactionMetadata() throws IOException {
        // 从元数据文件恢复事务的时间戳和隔离级别信息
        // 实际实现需要根据文件格式进行解析
    }

    private void saveTransactionMetadata(long xid, long timestamp, IsolationLevel level) throws IOException {
        // 计算偏移量
        long offset = TX_META_HEADER_SIZE + (xid - 1) * (TIMESTAMP_SIZE + ISOLATION_LEVEL_SIZE);

        // 写入时间戳和隔离级别
        ByteBuffer buffer = ByteBuffer.allocate(TIMESTAMP_SIZE + ISOLATION_LEVEL_SIZE);
        buffer.putLong(timestamp);
        buffer.putInt(level.getValue());
        buffer.flip();

        metaFC.write(buffer, offset);
        metaFC.force(false);
    }

    private void saveCommitTimestamp(long xid, long commitTS) throws IOException {
        // 在元数据文件中保存提交时间戳
        // 实际实现...
    }

    private void saveIsolationLevel(long xid, IsolationLevel level) throws IOException {
        // 计算偏移量
        long offset = TX_META_HEADER_SIZE + (xid - 1) * (TIMESTAMP_SIZE + ISOLATION_LEVEL_SIZE) + TIMESTAMP_SIZE;

        // 写入隔离级别
        ByteBuffer buffer = ByteBuffer.allocate(ISOLATION_LEVEL_SIZE);
        buffer.putInt(level.getValue());
        buffer.flip();

        metaFC.write(buffer, offset);
        metaFC.force(false);
    }

    // ---- 辅助方法 - 锁管理 ----

    /**
     * 锁类型枚举
     */
    public enum LockType {
        SHARED,       // 共享锁（读锁）
        EXCLUSIVE     // 排他锁（写锁）
    }

    /**
     * 获取锁
     */
    private boolean acquireLock(long xid, Object resourceID, LockType lockType, long timeout) throws DeadlockException {
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
            // 调用公共的abort方法中止事务
            abort(xid);
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 处理等待队列，尝试授予锁
     */
    private void processWaitQueue(LockEntry lockEntry, Object resourceID) {
        // 使用队列副本，避免并发修改异常
        Queue<LockRequest> waitQueue = new LinkedList<>(lockEntry.waitQueue);

        // 尝试为等待队列中的请求授予锁
        for (LockRequest request : waitQueue) {
            if (isLockCompatible(lockEntry, request.xid, request.lockType)) {
                grantLock(lockEntry, request.xid, request.lockType);
                updateTxLocks(request.xid, resourceID);

                // 更新等待图
                cleanupWaitForGraph(request.xid);
            } else {
                // 如果不能授予锁，停止处理（保持FIFO顺序）
                break;
            }
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
    private static class LockRequest {
        final long xid;
        final LockType lockType;

        LockRequest(long xid, LockType lockType) {
            this.xid = xid;
            this.lockType = lockType;
        }
    }
}
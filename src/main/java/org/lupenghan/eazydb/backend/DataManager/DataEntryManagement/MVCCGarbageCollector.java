package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement;

import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MVCC垃圾回收器
 * 负责清理不再需要的旧版本记录
 */
public class MVCCGarbageCollector {
    // 版本清理的安全边界（毫秒）
    private static final long SAFETY_MARGIN = 60000; // 1分钟

    // MVCC记录管理器
    private final MVCCRecordManager recordManager;

    // 事务管理器
    private final TransactionManager txManager;

    // 调度执行器
    private final ScheduledExecutorService executor;

    // 是否运行中
    private boolean running = false;

    /**
     * 创建MVCC垃圾回收器
     * @param recordManager MVCC记录管理器
     * @param txManager 事务管理器
     */
    public MVCCGarbageCollector(MVCCRecordManager recordManager, TransactionManager txManager) {
        this.recordManager = recordManager;
        this.txManager = txManager;
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * 启动垃圾回收器
     * @param initialDelay 初始延迟（秒）
     * @param period 周期（秒）
     */
    public void start(long initialDelay, long period) {
        if (running) {
            return;
        }

        running = true;
        executor.scheduleAtFixedRate(this::collectGarbage, initialDelay, period, TimeUnit.SECONDS);
    }

    /**
     * 停止垃圾回收器
     */
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        executor.shutdown();
    }

    /**
     * 执行垃圾回收
     */
    private void collectGarbage() {
        try {
            // 计算安全清理时间点
            long safeTimestamp = calculateSafeTimestamp();

            // 执行清理
            int purgedCount = recordManager.purgeOldVersions(safeTimestamp);

            System.out.println("MVCC垃圾回收完成，清理了 " + purgedCount + " 个过期版本");
        } catch (Exception e) {
            System.err.println("MVCC垃圾回收失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 计算安全的清理时间点
     * 只有早于此时间点的版本才会被清理
     * @return 安全时间点
     */
    private long calculateSafeTimestamp() {
        // 获取所有活跃事务
        java.util.Set<Long> activeTransactions = txManager.getActiveTransactions();

        // 找到最早的活跃事务开始时间戳
        long earliestTS = Long.MAX_VALUE;
        for (Long xid : activeTransactions) {
            long beginTS = txManager.getBeginTimestamp(xid);
            if (beginTS < earliestTS) {
                earliestTS = beginTS;
            }
        }

        // 如果没有活跃事务，使用当前时间减去安全边界
        if (earliestTS == Long.MAX_VALUE) {
            earliestTS = System.currentTimeMillis() - SAFETY_MARGIN;
        }

        return earliestTS;
    }

    /**
     * 手动触发垃圾回收
     * @return 清理的版本数量
     * @throws Exception 如果清理失败
     */
    public int forcedCollect() throws Exception {
        long safeTimestamp = calculateSafeTimestamp();
        return recordManager.purgeOldVersions(safeTimestamp);
    }
}
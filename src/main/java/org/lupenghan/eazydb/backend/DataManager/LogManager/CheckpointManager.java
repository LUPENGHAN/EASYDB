package org.lupenghan.eazydb.backend.DataManager.LogManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;

/**
 * 检查点管理 - LogManagerImpl的扩展方法
 */
public class CheckpointManager {
    private static final Logger LOGGER = Logger.getLogger(CheckpointManager.class.getName());

    // 日志类型常量
    private static final byte LOG_TYPE_BEGIN_CHECKPOINT = 5;  // 检查点开始
    private static final byte LOG_TYPE_END_CHECKPOINT = 4;    // 检查点结束

    // 事务状态常量
    private static final byte TX_STATUS_ACTIVE = 0;
    private static final byte TX_STATUS_COMMITTED = 1;
    private static final byte TX_STATUS_ABORTED = 2;

    private final LogManager logManager;
    private final TransactionManager txManager;

    /**
     * 创建检查点管理器
     * @param logManager 日志管理器
     * @param txManager 事务管理器
     */
    public CheckpointManager(LogManager logManager, TransactionManager txManager) {
        this.logManager = logManager;
        this.txManager = txManager;
    }

    /**
     * 创建检查点
     * @param activeTransactions 活跃事务表
     * @param dirtyPages 脏页表
     */
    public void createCheckpoint(Map<Long, TransactionInfo> activeTransactions, Map<PageID, Long> dirtyPages) {
        LOGGER.info("开始创建检查点...");

        try {
            // 1. 写入Begin Checkpoint日志
            long beginCpLSN = writeBeginCheckpointLog();

            // 2. 获取当前活跃事务
            if (activeTransactions == null) {
                activeTransactions = new ConcurrentHashMap<>();
                // 从事务管理器获取活跃事务
                // 这里需要根据实际的事务管理器接口来实现
            }

            // 3. 写入End Checkpoint日志，包含活跃事务表和脏页表
            writeEndCheckpointLog(beginCpLSN, activeTransactions, dirtyPages);

            // 4. 更新检查点位置
            // 这一步通常由logManager.checkpoint()方法内部处理

            LOGGER.info("检查点创建完成");
        } catch (Exception e) {
            LOGGER.severe("创建检查点失败: " + e.getMessage());
        }
    }

    /**
     * 写入Begin Checkpoint日志
     * @return 日志LSN
     */
    private long writeBeginCheckpointLog() {
        // 创建Begin Checkpoint日志记录
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(LOG_TYPE_BEGIN_CHECKPOINT);
        buffer.put((byte) 0); // 保留字节
        buffer.putShort((short) 0); // 保留字节

        byte[] data = buffer.array();

        // 追加日志
        long lsn = appendLogRecord(LOG_TYPE_BEGIN_CHECKPOINT, data);
        LOGGER.info("写入Begin Checkpoint日志, LSN=" + lsn);

        return lsn;
    }

    /**
     * 写入End Checkpoint日志
     * @param beginLSN Begin Checkpoint日志的LSN
     * @param activeTransactions 活跃事务表
     * @param dirtyPages 脏页表
     * @return 日志LSN
     */
    private long writeEndCheckpointLog(long beginLSN, Map<Long, TransactionInfo> activeTransactions, Map<PageID, Long> dirtyPages) {
        // 计算日志大小
        int txCount = activeTransactions != null ? activeTransactions.size() : 0;
        int pageCount = dirtyPages != null ? dirtyPages.size() : 0;

        // 基本大小: 类型(1) + beginLSN(8) + 事务数(4) + 脏页数(4)
        int logSize = 1 + 8 + 4 + 4;

        // 事务表大小: 每个事务的xid(8) + 状态(1) + lastLSN(8) + undoLSN数量(4) + undoLSNs大小
        if (activeTransactions != null) {
            for (Map.Entry<Long, TransactionInfo> entry : activeTransactions.entrySet()) {
                TransactionInfo txInfo = entry.getValue();
                logSize += 8 + 1 + 8 + 4 + (txInfo.undoLSNs.size() * 8);
            }
        }

        // 脏页表大小: 每个页面的fileID(4) + pageNum(4) + recLSN(8)
        logSize += pageCount * (4 + 4 + 8);

        // 创建缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(logSize);

        // 写入日志类型
        buffer.put(LOG_TYPE_END_CHECKPOINT);

        // 写入Begin Checkpoint LSN
        buffer.putLong(beginLSN);

        // 写入事务表
        buffer.putInt(txCount);
        if (activeTransactions != null) {
            for (Map.Entry<Long, TransactionInfo> entry : activeTransactions.entrySet()) {
                long xid = entry.getKey();
                TransactionInfo txInfo = entry.getValue();

                buffer.putLong(xid);

                // 确定事务状态
                byte status;
                if (txInfo.committed) {
                    status = TX_STATUS_COMMITTED;
                } else if (txInfo.aborted) {
                    status = TX_STATUS_ABORTED;
                } else {
                    status = TX_STATUS_ACTIVE;
                }
                buffer.put(status);

                // 写入最后一条日志的LSN
                buffer.putLong(txInfo.lastLSN);

                // 写入UNDO日志列表
                buffer.putInt(txInfo.undoLSNs.size());
                for (Long undoLSN : txInfo.undoLSNs) {
                    buffer.putLong(undoLSN);
                }
            }
        }

        // 写入脏页表
        buffer.putInt(pageCount);
        if (dirtyPages != null) {
            for (Map.Entry<PageID, Long> entry : dirtyPages.entrySet()) {
                PageID pageID = entry.getKey();
                long recLSN = entry.getValue();

                buffer.putInt(pageID.getFileID());
                buffer.putInt(pageID.getPageNum());
                buffer.putLong(recLSN);
            }
        }

        // 获取完整的日志数据
        byte[] data = buffer.array();

        // 追加日志
        long lsn = appendLogRecord(LOG_TYPE_END_CHECKPOINT, data);
        LOGGER.info("写入End Checkpoint日志, LSN=" + lsn);

        return lsn;
    }

    /**
     * 将日志记录追加到日志文件
     * @param logType 日志类型
     * @param data 日志数据
     * @return 日志LSN
     */
    private long appendLogRecord(byte logType, byte[] data) {
        // 由于这是一个辅助类，我们需要调用LogManager的方法
        // 根据日志类型调用不同的方法

        try {
            // 这里简化处理，实际上应该根据logType区分不同类型的日志
            // 我们创建一个特殊的空操作UNDO日志
            return logManager.appendUndoLog(-1, logType, data);
        } catch (Exception e) {
            LOGGER.severe("追加日志记录失败: " + e.getMessage());
            return -1;
        }
    }

    /**
     * 内部类：事务信息，与RecoveryManager中的定义匹配
     */
    public static class TransactionInfo {
        public boolean committed;
        public boolean aborted;
        public long lastLSN;
        public List<Long> undoLSNs;

        public TransactionInfo() {
            this.committed = false;
            this.aborted = false;
            this.lastLSN = 0;
            this.undoLSNs = new ArrayList<>();
        }
    }
}
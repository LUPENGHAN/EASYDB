package org.lupenghan.eazydb.backend.DataManager.LogManager;

import org.lupenghan.eazydb.backend.DataManager.LogManager.DataForm.RedoLogRecord;
import org.lupenghan.eazydb.backend.DataManager.LogManager.DataForm.UndoLogRecord;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * 恢复管理器 - 实现ARIES（Algorithm for Recovery and Isolation Exploiting Semantics）恢复算法
 */
public class RecoveryManager {
    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());

    // 日志记录类型
    private static final byte LOG_TYPE_COMPENSATION = 3;  // 补偿日志
    private static final byte LOG_TYPE_END_CHECKPOINT = 4;  // 检查点结束
    private static final byte LOG_TYPE_BEGIN_CHECKPOINT = 5;  // 检查点开始

    // 操作类型常量
    private static final int UNDO_INSERT = UndoLogRecord.INSERT;
    private static final int UNDO_DELETE = UndoLogRecord.DELETE;
    private static final int UNDO_UPDATE = UndoLogRecord.UPDATE;

    /**
     * 事务信息内部类，用于恢复过程中跟踪事务状态
     */
    private static class TransactionInfo {
        boolean committed;         // 事务是否已提交
        boolean aborted;           // 事务是否已中止
        long lastLSN;              // 事务最后一条日志的LSN
        List<Long> undoLSNs;       // 事务的所有需要撤销的日志LSN

        public TransactionInfo() {
            this.committed = false;
            this.aborted = false;
            this.lastLSN = 0;
            this.undoLSNs = new ArrayList<>();
        }
    }

    // 活跃事务表（Active Transaction Table, ATT）
    private Map<Long, TransactionInfo> activeTransactions;

    // 脏页表（Dirty Page Table, DPT）
    private Map<PageID, Long> dirtyPages;  // 页面ID -> RecLSN

    private LogManager logManager;
    private PageManager pageManager;
    private TransactionManager txManager;

    /**
     * 创建恢复管理器
     * @param logManager 日志管理器
     * @param pageManager 页面管理器
     * @param txManager 事务管理器
     */
    public RecoveryManager(LogManager logManager, PageManager pageManager, TransactionManager txManager) {
        this.logManager = logManager;
        this.pageManager = pageManager;
        this.txManager = txManager;
        this.activeTransactions = new ConcurrentHashMap<>();
        this.dirtyPages = new ConcurrentHashMap<>();
    }

    /**
     * 执行恢复过程
     */
    public void recover() {
        LOGGER.info("开始数据库恢复过程...");

        // 1. 分析阶段
        LOGGER.info("开始分析阶段...");
        analyzePhase();

        // 2. 重做阶段
        LOGGER.info("开始重做阶段...");
        redoPhase();

        // 3. 撤销阶段
        LOGGER.info("开始撤销阶段...");
        undoPhase();

        // 4. 创建新检查点
        LOGGER.info("创建新检查点...");
        logManager.checkpoint();

        LOGGER.info("数据库恢复完成");
    }

    /**
     * 分析阶段 - 扫描日志重建恢复所需数据结构
     */
    private void analyzePhase() {
        // 重置数据结构
        activeTransactions.clear();
        dirtyPages.clear();

        // 获取日志迭代器（从最近的检查点开始扫描）
        LogManager.LogIterator iterator = logManager.iterator();

        // 最新的检查点信息
        Map<Long, TransactionInfo> checkpointTransactions = null;
        Map<PageID, Long> checkpointDirtyPages = null;

        // 扫描日志
        while (iterator.hasNext()) {
            byte[] logData = iterator.next();
            long lsn = iterator.position() - logData.length;

            // 判断日志类型
            if (iterator.isRedo()) {
                // 处理REDO日志
                RedoLogRecord redoRecord = RedoLogRecord.deserialize(logData);
                long xid = redoRecord.getXid();

                // 更新事务表
                TransactionInfo txInfo = activeTransactions.computeIfAbsent(xid, k -> new TransactionInfo());
                txInfo.lastLSN = lsn;

                // 更新脏页表（仅当该页面未在脏页表中时添加）
                PageID pageID = new PageID(-1, redoRecord.getPageID()); // 假设使用页号作为唯一标识
                dirtyPages.putIfAbsent(pageID, lsn);

            } else {
                // 处理UNDO或其他类型日志
                ByteBuffer buffer = ByteBuffer.wrap(logData);
                byte logType = buffer.get(); // 读取日志类型

                if (logType == LOG_TYPE_BEGIN_CHECKPOINT) {
                    // 检查点开始日志，跳过
                    continue;
                } else if (logType == LOG_TYPE_END_CHECKPOINT) {
                    // 检查点结束日志，读取检查点信息
                    checkpointTransactions = readTransactionTableFromCheckpoint(logData);
                    checkpointDirtyPages = readDirtyPagesFromCheckpoint(logData);
                } else {
                    // 假设是UndoLog或其他类型
                    UndoLogRecord undoRecord = UndoLogRecord.deserialize(logData);
                    long xid = undoRecord.getXid();

                    // 更新事务表
                    TransactionInfo txInfo = activeTransactions.computeIfAbsent(xid, k -> new TransactionInfo());
                    txInfo.lastLSN = lsn;

                    // 根据操作类型判断
                    int opType = undoRecord.getOperationType();
                    if (opType == -1) { // 假设-1表示事务提交
                        txInfo.committed = true;
                    } else if (opType == -2) { // 假设-2表示事务中止
                        txInfo.aborted = true;
                    } else {
                        // 普通UNDO日志，记录用于回滚
                        txInfo.undoLSNs.add(lsn);
                    }
                }
            }
        }

        // 合并检查点信息（如果有）
        if (checkpointTransactions != null) {
            for (Map.Entry<Long, TransactionInfo> entry : checkpointTransactions.entrySet()) {
                long xid = entry.getKey();
                TransactionInfo checkpointTxInfo = entry.getValue();

                // 只有在当前活跃事务表中没有该事务的更新信息时，才使用检查点中的信息
                if (!activeTransactions.containsKey(xid)) {
                    activeTransactions.put(xid, checkpointTxInfo);
                }
            }
        }

        if (checkpointDirtyPages != null) {
            for (Map.Entry<PageID, Long> entry : checkpointDirtyPages.entrySet()) {
                PageID pageID = entry.getKey();
                long recLSN = entry.getValue();

                // 只有在当前脏页表中没有该页面的更新信息时，才使用检查点中的信息
                if (!dirtyPages.containsKey(pageID)) {
                    dirtyPages.put(pageID, recLSN);
                }
            }
        }

        // 清除已提交或已中止的事务
        Iterator<Map.Entry<Long, TransactionInfo>> it = activeTransactions.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, TransactionInfo> entry = it.next();
            TransactionInfo txInfo = entry.getValue();
            if (txInfo.committed || txInfo.aborted) {
                it.remove();
            } else {
                // 将未完成事务标记为中止
                long xid = entry.getKey();
                txManager.abort(xid);
            }
        }

        LOGGER.info("分析阶段完成，活跃事务数: " + activeTransactions.size() + ", 脏页数: " + dirtyPages.size());
    }

    /**
     * 从检查点日志中读取事务表
     */
    private Map<Long, TransactionInfo> readTransactionTableFromCheckpoint(byte[] logData) {
        // 解析检查点日志中的事务表信息
        // 这里需要根据实际的检查点日志格式实现
        // 示例代码仅作参考
        Map<Long, TransactionInfo> txTable = new HashMap<>();

        try {
            ByteBuffer buffer = ByteBuffer.wrap(logData);
            buffer.get(); // 跳过日志类型

            // 读取事务数量
            int txCount = buffer.getInt();

            // 读取每个事务的信息
            for (int i = 0; i < txCount; i++) {
                long xid = buffer.getLong();
                byte status = buffer.get(); // 0-活跃，1-已提交，2-已中止
                long lastLSN = buffer.getLong();

                TransactionInfo txInfo = new TransactionInfo();
                txInfo.committed = (status == 1);
                txInfo.aborted = (status == 2);
                txInfo.lastLSN = lastLSN;

                // 读取UNDO日志数量
                int undoCount = buffer.getInt();
                for (int j = 0; j < undoCount; j++) {
                    txInfo.undoLSNs.add(buffer.getLong());
                }

                txTable.put(xid, txInfo);
            }
        } catch (Exception e) {
            LOGGER.warning("解析检查点事务表失败: " + e.getMessage());
        }

        return txTable;
    }

    /**
     * 从检查点日志中读取脏页表
     */
    private Map<PageID, Long> readDirtyPagesFromCheckpoint(byte[] logData) {
        // 解析检查点日志中的脏页表信息
        // 这里需要根据实际的检查点日志格式实现
        // 示例代码仅作参考
        Map<PageID, Long> dirtyPageTable = new HashMap<>();

        try {
            ByteBuffer buffer = ByteBuffer.wrap(logData);
            buffer.get(); // 跳过日志类型

            // 跳过事务表信息
            int txCount = buffer.getInt();
            for (int i = 0; i < txCount; i++) {
                buffer.getLong(); // xid
                buffer.get(); // status
                buffer.getLong(); // lastLSN

                int undoCount = buffer.getInt();
                buffer.position(buffer.position() + undoCount * 8); // 跳过UNDO LSN列表
            }

            // 读取脏页数量
            int pageCount = buffer.getInt();

            // 读取每个脏页的信息
            for (int i = 0; i < pageCount; i++) {
                int fileID = buffer.getInt();
                int pageNum = buffer.getInt();
                long recLSN = buffer.getLong();

                PageID pageID = new PageID(fileID, pageNum);
                dirtyPageTable.put(pageID, recLSN);
            }
        } catch (Exception e) {
            LOGGER.warning("解析检查点脏页表失败: " + e.getMessage());
        }

        return dirtyPageTable;
    }

    /**
     * 重做阶段 - 从日志重放修改操作
     */
    private void redoPhase() {
        // 设置重做起点（脏页表中最小的RecLSN）
        long redoStartLSN = Long.MAX_VALUE;
        for (long recLSN : dirtyPages.values()) {
            if (recLSN < redoStartLSN) {
                redoStartLSN = recLSN;
            }
        }

        if (redoStartLSN == Long.MAX_VALUE) {
            redoStartLSN = 0; // 如果脏页表为空，从头开始
        }

        LOGGER.info("重做起点LSN: " + redoStartLSN);

        // 从重做起点开始扫描日志
        LogManager.LogIterator iterator = logManager.iterator();

        // 跳过不需要重做的日志
        long currentLSN = 0;
        while (iterator.hasNext() && currentLSN < redoStartLSN) {
            byte[] logData = iterator.next();
            currentLSN = iterator.position();
        }

        // 开始重做符合条件的日志
        while (iterator.hasNext()) {
            byte[] logData = iterator.next();
            long lsn = iterator.position() - logData.length;

            if (!iterator.isRedo()) {
                continue; // 跳过非REDO日志
            }

            // 解析REDO日志
            RedoLogRecord record = RedoLogRecord.deserialize(logData);
            int pageNum = record.getPageID();
            PageID pageID = new PageID(-1, pageNum); // 假设使用页号作为唯一标识

            // 确定是否需要重做该日志
            Long recLSN = dirtyPages.get(pageID);
            if (recLSN != null && lsn >= recLSN) {
                // 需要重做，获取页面
                Page page = pageManager.pinPage(pageID);

                if (page != null) {
                    // 检查页面LSN是否小于日志LSN
                    if (page.getLSN() < lsn) {
                        LOGGER.info("重做日志 LSN=" + lsn + " 对页面 " + pageID);

                        // 执行页面修改
                        short offset = record.getOffset();
                        byte[] newData = record.getNewData();
                        page.writeData(offset, newData);

                        // 更新页面LSN
                        page.setLSN(lsn);
                    }

                    // 释放页面
                    pageManager.unpinPage(pageID, true);
                } else {
                    LOGGER.warning("无法获取页面 " + pageID + " 进行重做");
                }
            }
        }

        LOGGER.info("重做阶段完成");
    }

    /**
     * 撤销阶段 - 回滚未完成事务
     */
    private void undoPhase() {
        if (activeTransactions.isEmpty()) {
            LOGGER.info("没有需要撤销的活跃事务");
            return;
        }

        LOGGER.info("需要撤销的事务数: " + activeTransactions.size());

        // 按LSN递减顺序处理所有活跃事务的UNDO日志
        PriorityQueue<UndoLogItem> undoQueue = new PriorityQueue<>(
                Comparator.comparingLong(UndoLogItem::getLsn).reversed());

        // 初始化UNDO队列
        for (Map.Entry<Long, TransactionInfo> entry : activeTransactions.entrySet()) {
            long xid = entry.getKey();
            TransactionInfo txInfo = entry.getValue();

            if (txInfo.lastLSN > 0) {
                undoQueue.add(new UndoLogItem(xid, txInfo.lastLSN));
            }
        }

        // 处理UNDO队列
        while (!undoQueue.isEmpty()) {
            UndoLogItem item = undoQueue.poll();
            long lsn = item.getLsn();
            long xid = item.getXid();

            // 读取UNDO日志
            byte[] logData = logManager.readUndoLog(lsn);
            if (logData == null) {
                LOGGER.warning("无法读取UNDO日志 LSN=" + lsn);
                continue;
            }

            UndoLogRecord record = UndoLogRecord.deserialize(logData);

            // 执行UNDO操作
            LOGGER.info("撤销操作 XID=" + xid + " LSN=" + lsn + " 操作类型=" + record.getOperationType());
            undoOperation(record);

            // 写入补偿日志（CLR）
            // long clrLSN = writeCompensationLog(xid, record);

            // 获取前驱LSN（PrevLSN）
            long prevLSN = getPrevLSN(xid, lsn);
            if (prevLSN > 0) {
                // 加入队列继续处理
                undoQueue.add(new UndoLogItem(xid, prevLSN));
            } else {
                // 写入事务结束日志
                // writeEndTransactionLog(xid);
                LOGGER.info("事务 " + xid + " 撤销完成");
            }
        }

        LOGGER.info("撤销阶段完成");
    }

    /**
     * UNDO日志项，用于撤销阶段
     */
    private static class UndoLogItem {
        private final long xid;
        private final long lsn;

        public UndoLogItem(long xid, long lsn) {
            this.xid = xid;
            this.lsn = lsn;
        }

        public long getXid() {
            return xid;
        }

        public long getLsn() {
            return lsn;
        }
    }

    /**
     * 执行UNDO操作
     */
    private void undoOperation(UndoLogRecord record) {
        int opType = record.getOperationType();
        byte[] undoData = record.getUndoData();

        switch (opType) {
            case UNDO_INSERT:
                undoInsert(undoData);
                break;
            case UNDO_DELETE:
                undoDelete(undoData);
                break;
            case UNDO_UPDATE:
                undoUpdate(undoData);
                break;
            default:
                LOGGER.warning("未知的操作类型: " + opType);
        }
    }

    /**
     * 撤销插入操作
     */
    private void undoInsert(byte[] undoData) {
        // 从UNDO数据中提取记录位置信息，然后删除该记录
        // 具体实现取决于记录管理器接口
        ByteBuffer buffer = ByteBuffer.wrap(undoData);

        // 示例格式：[fileID:4字节][pageNum:4字节][slotNum:4字节]
        int fileID = buffer.getInt();
        int pageNum = buffer.getInt();
        int slotNum = buffer.getInt();

        PageID pageID = new PageID(fileID, pageNum);

        try {
            // 调用记录管理器删除记录
            // recordManager.deleteRecord(new RecordID(pageID, slotNum), 0); // 使用系统事务ID 0
            LOGGER.info("撤销插入: 页面=" + pageID + " 槽=" + slotNum);
        } catch (Exception e) {
            LOGGER.warning("撤销插入失败: " + e.getMessage());
        }
    }

    /**
     * 撤销删除操作
     */
    private void undoDelete(byte[] undoData) {
        // 从UNDO数据中提取记录信息，然后恢复该记录
        // 具体实现取决于记录管理器接口
        ByteBuffer buffer = ByteBuffer.wrap(undoData);

        // 示例格式：[fileID:4字节][pageNum:4字节][slotNum:4字节][dataLength:4字节][data:变长]
        int fileID = buffer.getInt();
        int pageNum = buffer.getInt();
        int slotNum = buffer.getInt();
        int dataLength = buffer.getInt();

        byte[] recordData = new byte[dataLength];
        buffer.get(recordData);

        PageID pageID = new PageID(fileID, pageNum);

        try {
            // 调用记录管理器恢复记录
            // recordManager.restoreRecord(new RecordID(pageID, slotNum), recordData, 0); // 使用系统事务ID 0
            LOGGER.info("撤销删除: 页面=" + pageID + " 槽=" + slotNum);
        } catch (Exception e) {
            LOGGER.warning("撤销删除失败: " + e.getMessage());
        }
    }

    /**
     * 撤销更新操作
     */
    private void undoUpdate(byte[] undoData) {
        // 从UNDO数据中提取记录位置和原始数据，然后恢复原始数据
        // 具体实现取决于记录管理器接口
        ByteBuffer buffer = ByteBuffer.wrap(undoData);

        // 示例格式：[fileID:4字节][pageNum:4字节][slotNum:4字节][dataLength:4字节][oldData:变长]
        int fileID = buffer.getInt();
        int pageNum = buffer.getInt();
        int slotNum = buffer.getInt();
        int dataLength = buffer.getInt();

        byte[] oldData = new byte[dataLength];
        buffer.get(oldData);

        PageID pageID = new PageID(fileID, pageNum);

        try {
            // 调用记录管理器更新记录为原始状态
            // recordManager.updateRecord(new RecordID(pageID, slotNum), oldData, 0); // 使用系统事务ID 0
            LOGGER.info("撤销更新: 页面=" + pageID + " 槽=" + slotNum);
        } catch (Exception e) {
            LOGGER.warning("撤销更新失败: " + e.getMessage());
        }
    }

    /**
     * 获取指定事务和LSN之前的LSN
     */
    private long getPrevLSN(long xid, long currentLSN) {
        TransactionInfo txInfo = activeTransactions.get(xid);
        if (txInfo == null || txInfo.undoLSNs.isEmpty()) {
            return 0;
        }

        long prevLSN = 0;
        for (Long lsn : txInfo.undoLSNs) {
            if (lsn < currentLSN && lsn > prevLSN) {
                prevLSN = lsn;
            }
        }

        return prevLSN;
    }
}
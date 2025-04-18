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

public class MVCCTransactionManagerImpl implements MVCCTransactionManager {
    // 从原有TransactionManagerImpl继承的常量
    private static final byte ACTIVE = 1;
    private static final byte COMMITTED = 2;
    private static final byte ABORT = 3;
    private static final int XID_HEADER_LENGTH = 8;
    private static final int XID_FIELD_SIZE = 1;

    // MVCC相关的常量
    private static final int ISOLATION_LEVEL_SIZE = 4;
    private static final int TIMESTAMP_SIZE = 8;

    // 事务元数据文件格式定义
    private static final int TX_META_HEADER_SIZE = 8; // 元数据文件头大小

    // 文件和通道
    private RandomAccessFile xidFile;
    private RandomAccessFile metaFile; // 新增的事务元数据文件
    private FileChannel xidFC;
    private FileChannel metaFC;
    private long xidCounter;

    // 缓存
    private Set<Long> cachedCommittedTransactions;

    // MVCC相关的缓存
    private final Map<Long, Long> beginTimestamps;     // 事务ID -> 开始时间戳
    private final Map<Long, Long> commitTimestamps;    // 事务ID -> 提交时间戳
    private final Map<Long, IsolationLevel> isolationLevels;  // 事务ID -> 隔离级别
    private final Map<Long, ReadView> readViews;      // 事务ID -> ReadView（可重复读使用）

    /**
     * 创建MVCC事务管理器
     * @param path 数据库路径
     * @throws Exception 如果创建失败
     */
    public MVCCTransactionManagerImpl(String path) throws Exception {
        // 初始化原有事务管理器功能
        File xidFile = new File(path + "/xid.txn");
        boolean isNewXidFile = !xidFile.exists();

        File metaFile = new File(path + "/txmeta.dat");
        boolean isNewMetaFile = !metaFile.exists();

        // 确保目录存在
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.xidFile = new RandomAccessFile(xidFile, "rw");
        this.xidFC = this.xidFile.getChannel();

        this.metaFile = new RandomAccessFile(metaFile, "rw");
        this.metaFC = this.metaFile.getChannel();

        if (isNewXidFile) {
            // 新文件，初始化XID为1（0是保留的无效事务ID）
            ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
            buffer.putLong(1);
            buffer.flip();
            xidFC.write(buffer, 0);
            xidCounter = 1;
        } else {
            // 已有文件，读取现有XID计数器
            ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
            xidFC.read(buf, 0);
            buf.flip();
            xidCounter = buf.getLong();
        }

        if (isNewMetaFile) {
            // 初始化元数据文件头
            ByteBuffer buffer = ByteBuffer.wrap(new byte[TX_META_HEADER_SIZE]);
            buffer.putLong(0); // 预留
            buffer.flip();
            metaFC.write(buffer, 0);
        }

        // 初始化缓存
        cachedCommittedTransactions = new HashSet<>();
        beginTimestamps = new ConcurrentHashMap<>();
        commitTimestamps = new ConcurrentHashMap<>();
        isolationLevels = new ConcurrentHashMap<>();
        readViews = new ConcurrentHashMap<>();

        // 恢复数据
        if (!isNewXidFile) {
            recoverCommittedTransactions();
        }

        if (!isNewMetaFile) {
            recoverTransactionMetadata();
        }
    }

    /**
     * 恢复事务元数据
     */
    private void recoverTransactionMetadata() throws IOException {
        // 从元数据文件恢复事务的时间戳和隔离级别信息
        // 实际实现需要根据文件格式进行解析

        // 示例实现...
    }

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
            xidFC.write(buf, 0);
            xidFC.force(false); // 确保持久化

            // 保存事务元数据
            saveTransactionMetadata(xid, timestamp, IsolationLevel.REPEATABLE_READ);

        } catch (IOException e) {
            throw new RuntimeException("Failed to begin transaction", e);
        }

        return xid;
    }

    /**
     * 保存事务元数据
     */
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

        try {
            // 保存提交时间戳
            saveCommitTimestamp(xid, commitTS);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save commit timestamp", e);
        }
    }

    /**
     * 保存提交时间戳
     */
    private void saveCommitTimestamp(long xid, long commitTS) throws IOException {
        // 在元数据文件中保存提交时间戳
        // 实际实现...
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
    public void close() {
        try {
            xidFile.close();
            xidFC.close();
            metaFile.close();
            metaFC.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close transaction files", e);
        }
    }

    // 辅助方法
    private void updateXID(long xid, byte status) {
        long offset = XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        buffer.put(status);
        buffer.flip();
        try {
            xidFC.write(buffer, offset);
            xidFC.force(false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update XID(transaction state)", e);
        }
    }

    private void recoverCommittedTransactions() throws IOException {
        long fileSize = xidFC.size();
        long xidCount = (fileSize - XID_HEADER_LENGTH) / XID_FIELD_SIZE;

        // 批量读取状态提高性能
        ByteBuffer buffer = ByteBuffer.allocate((int)fileSize - XID_HEADER_LENGTH);
        xidFC.read(buffer, XID_HEADER_LENGTH);
        buffer.flip();
        for (long i = 0; i < xidCount; i++) {
            byte status = buffer.get();
            if (status == COMMITTED) {
                cachedCommittedTransactions.add(i + 1);
            }
        }
    }

    // 检查事务状态
    private boolean checkXIDState(long xid, byte state) {
        long offset = XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;

        // 如果事务ID超出文件范围，则状态一定是活跃的
        try {
            if (offset >= xidFC.size()) {
                return state == ACTIVE;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check file size", e);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        try {
            xidFC.read(buf, offset);
            buf.flip();
            return buf.get() == state;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read transaction state", e);
        }
    }

    // MVCC扩展方法实现

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

    /**
     * 保存隔离级别
     */
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

    @Override
    public IsolationLevel getIsolationLevel(long xid) {
        return isolationLevels.getOrDefault(xid, IsolationLevel.REPEATABLE_READ);
    }
}
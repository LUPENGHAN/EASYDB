package org.lupenghan.eazydb.backend.DataManager.LogManager;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.DataForm.LogRecord;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class LogManagerImpl implements LogManager {
    private static final Logger LOGGER = Logger.getLogger(LogManagerImpl.class.getName());

    // 日志文件头部大小，用于存储元数据
    private static final int LOG_HEADER_SIZE = 16; // 增加了字段，所以扩大了头部

    // 日志类型
    private static final byte LOG_TYPE_REDO = 0;
    private static final byte LOG_TYPE_UNDO = 1;
    private static final byte LOG_TYPE_CHECKPOINT = 2;
    private static final byte LOG_TYPE_COMPENSATION = 3;  // 补偿日志
    private static final byte LOG_TYPE_END_CHECKPOINT = 4;  // 检查点结束
    private static final byte LOG_TYPE_BEGIN_CHECKPOINT = 5;  // 检查点开始

    // 日志缓冲区大小
    private static final int LOG_BUFFER_SIZE = 4 * 1024 * 1024; // 4MB

    // 日志文件
    protected RandomAccessFile logFile;
    protected FileChannel fc;

    // 缓冲区
    protected ByteBuffer writeBuffer;

    // 并发控制
    protected Lock lock;

    // 当前日志位置
    protected long position;

    // 当前有效的检查点位置
    protected long checkpointPosition;

    // 增强版功能所需的组件
    protected CheckpointManager checkpointManager;
    protected TransactionManager txManager;
    protected PageManager pageManager;

    // 基本构造函数，兼容原有代码
    public LogManagerImpl(String path) throws IOException {
        initLogManager(path);
    }

    // 增强版构造函数，包含事务管理器和页面管理器
    public LogManagerImpl(String path, TransactionManager txManager, PageManager pageManager) throws IOException {
        initLogManager(path);
        this.txManager = txManager;
        this.pageManager = pageManager;
        if (txManager != null) {
            this.checkpointManager = new CheckpointManager(this, txManager);
        }
    }

    // 初始化日志管理器的共用方法
    private void initLogManager(String path) throws IOException {
        File file = new File(path + "/eazydb.log");
        boolean isNewFile = !file.exists();

        // 确保目录存在
        File dir = new File(path);
        if(!dir.exists()) {
            dir.mkdirs();
        }

        logFile = new RandomAccessFile(file, "rw");
        fc = logFile.getChannel();

        writeBuffer = ByteBuffer.allocate(LOG_BUFFER_SIZE);
        lock = new ReentrantLock();

        if(isNewFile) {
            // 初始化日志文件头
            // 文件头格式: [position(8)] [checkpointPosition(8)]
            ByteBuffer buf = ByteBuffer.allocate(LOG_HEADER_SIZE);
            buf.putLong(LOG_HEADER_SIZE); // 初始位置
            buf.putLong(0); // 初始检查点位置(0表示无检查点)
            buf.flip();
            fc.write(buf, 0);
            position = LOG_HEADER_SIZE;
            checkpointPosition = 0;
        } else {
            // 读取现有日志文件的位置指针和检查点
            ByteBuffer buf = ByteBuffer.allocate(LOG_HEADER_SIZE);
            fc.read(buf, 0);
            buf.flip();
            position = buf.getLong();
            checkpointPosition = buf.getLong();
        }
    }

    @Override
    public long appendRedoLog(long xid, int pageID, short offset, byte[] oldData, byte[] newData) {
        LogRecord record = LogRecord.createRedoLog(xid, pageID, offset, oldData, newData);
        byte[] data = record.serialize();
        return appendLogInternal(LOG_TYPE_REDO, data);
    }

    @Override
    public long appendUndoLog(long xid, int operationType, byte[] undoData) {
        LogRecord record = LogRecord.createUndoLog(xid, operationType, undoData);
        byte[] data = record.serialize();
        return appendLogInternal(LOG_TYPE_UNDO, data);
    }

    protected long appendLogInternal(byte logType, byte[] data) {
        lock.lock();
        try {
            // 日志记录格式: [type(1)][size(4)][data(N)]
            int logSize = 1 + 4 + data.length;

            // 如果缓冲区不足，先刷盘
            if(writeBuffer.remaining() < logSize) {
                flush();
            }

            // 记录当前日志位置
            long pos = position;

            // 写入日志类型
            writeBuffer.put(logType);
            // 写入日志大小
            writeBuffer.putInt(data.length);
            // 写入日志数据
            writeBuffer.put(data);

            // 更新位置
            position += logSize;

            // 如果缓冲区使用过半，触发异步刷盘
            if(writeBuffer.position() > LOG_BUFFER_SIZE / 2) {
                // 在实际实现中，这里应该是异步刷盘
                flush();
            }

            // 返回日志位置作为LSN
            return pos;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] readRedoLog(long lsn) {
        byte[] logData = readLogInternal(lsn);
        if(logData == null) return null;

        // 检查日志类型
        ByteBuffer buf = ByteBuffer.wrap(logData);
        byte type = buf.get();
        if(type != LOG_TYPE_REDO) {
            throw new RuntimeException("Not a REDO log at position: " + lsn);
        }

        // 跳过日志大小字段(4字节)
        int size = buf.getInt();

        // 提取实际的日志内容
        byte[] data = new byte[size];
        buf.get(data);

        return data;
    }

    @Override
    public byte[] readUndoLog(long lsn) {
        byte[] logData = readLogInternal(lsn);
        if(logData == null) return null;

        // 检查日志类型
        ByteBuffer buf = ByteBuffer.wrap(logData);
        byte type = buf.get();
        if(type != LOG_TYPE_UNDO) {
            throw new RuntimeException("Not an UNDO log at position: " + lsn);
        }

        // 跳过日志大小字段(4字节)
        int size = buf.getInt();

        // 提取实际的日志内容
        byte[] data = new byte[size];
        buf.get(data);

        return data;
    }

    // 读取指定位置的完整日志记录(包括类型和大小)
    protected byte[] readLogInternal(long position) {
        lock.lock();
        try {
            // 首先检查是否在缓冲区内
            if(position >= this.position - writeBuffer.position() && position < this.position) {
                // 从缓冲区读取
                int offset = (int)(position - (this.position - writeBuffer.position()));
                ByteBuffer buf = writeBuffer.duplicate();
                buf.position(offset);

                byte type = buf.get();
                int size = buf.getInt();

                // 创建结果数组，包含type和size
                byte[] result = new byte[1 + 4 + size];
                buf.position(offset);
                buf.get(result);

                return result;
            }

            // 否则从文件读取
            // 首先读取类型和大小
            ByteBuffer headerBuf = ByteBuffer.allocate(5); // type(1) + size(4)
            try {
                fc.read(headerBuf, position);
                headerBuf.flip();

                byte type = headerBuf.get();
                int size = headerBuf.getInt();

                // 准备结果数组
                byte[] result = new byte[1 + 4 + size];

                // 复制header
                System.arraycopy(headerBuf.array(), 0, result, 0, 5);

                // 读取数据部分
                ByteBuffer dataBuf = ByteBuffer.allocate(size);
                fc.read(dataBuf, position + 5);
                dataBuf.flip();

                // 复制数据
                System.arraycopy(dataBuf.array(), 0, result, 5, size);

                return result;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read log at position: " + position, e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush() {
        lock.lock();
        try {
            if(writeBuffer.position() == 0) {
                return; // 缓冲区为空，无需刷盘
            }

            writeBuffer.flip();
            try {
                fc.write(writeBuffer, position - writeBuffer.limit());

                // 更新文件头中的位置信息
                ByteBuffer buf = ByteBuffer.allocate(LOG_HEADER_SIZE);
                buf.putLong(position);
                buf.putLong(checkpointPosition);
                buf.flip();
                fc.write(buf, 0);

                // 强制刷盘
                fc.force(false);
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush log", e);
            }

            // 清空缓冲区
            writeBuffer.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void checkpoint() {
        // 如果有CheckpointManager则使用增强版的检查点创建
        if (checkpointManager != null && txManager != null && pageManager != null) {
            createEnhancedCheckpoint();
        } else {
            // 否则使用基本版的检查点创建
            createBasicCheckpoint();
        }
    }

    // 基本版检查点创建
    protected void createBasicCheckpoint() {
        lock.lock();
        try {
            // 首先确保所有缓冲区数据刷盘
            flush();

            // 创建检查点记录
            // 在实际实现中，检查点记录应该包含活跃事务信息等
            ByteBuffer buf = ByteBuffer.allocate(4); // 简单的检查点记录，仅包含一个标识
            buf.putInt(0xCCCCCCCC); // 检查点标识
            buf.flip();

            byte[] data = new byte[buf.limit()];
            buf.get(data);

            // 写入检查点记录
            long cp = appendLogInternal(LOG_TYPE_CHECKPOINT, data);

            // 更新检查点位置
            checkpointPosition = cp;

            // 更新文件头
            ByteBuffer headerBuf = ByteBuffer.allocate(LOG_HEADER_SIZE);
            headerBuf.putLong(position);
            headerBuf.putLong(checkpointPosition);
            headerBuf.flip();

            try {
                fc.write(headerBuf, 0);
                fc.force(true);
            } catch (IOException e) {
                throw new RuntimeException("Failed to update checkpoint", e);
            }
        } finally {
            lock.unlock();
        }
    }

    // 增强版检查点创建
    protected void createEnhancedCheckpoint() {
        LOGGER.info("开始创建增强版检查点...");

        try {
            lock.lock();

            try {
                // 1. 首先刷新所有日志缓冲区内容到磁盘
                flush();

                // 2. 获取活跃事务和脏页信息
                Map<Long, CheckpointManager.TransactionInfo> activeTransactions = getActiveTransactions();
                Map<PageID, Long> dirtyPages = getDirtyPages();

                // 3. 使用检查点管理器创建包含完整信息的检查点
                checkpointManager.createCheckpoint(activeTransactions, dirtyPages);

                // 4. 刷新所有页面到磁盘
                pageManager.flushAll();

                // 5. 更新检查点位置
                // 这会由基本的checkpoint方法完成，所以我们调用它
                createBasicCheckpoint();

                LOGGER.info("增强版检查点创建完成");
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            LOGGER.severe("创建检查点失败: " + e.getMessage());
        }
    }

    /**
     * 获取当前活跃事务
     */
    protected Map<Long, CheckpointManager.TransactionInfo> getActiveTransactions() {
        // 演示用，实际实现应该从事务管理器获取
        Map<Long, CheckpointManager.TransactionInfo> result = new ConcurrentHashMap<>();

        // 假设事务ID从1到100，检查哪些是活跃的
        for (long xid = 1; xid <= 100; xid++) {
            if (txManager != null && txManager.isActive(xid)) {
                CheckpointManager.TransactionInfo txInfo = new CheckpointManager.TransactionInfo();
                txInfo.lastLSN = 0; // 在实际实现中，需要获取最后一条日志的LSN
                result.put(xid, txInfo);
            }
        }

        return result;
    }

    /**
     * 获取当前脏页信息
     */
    protected Map<PageID, Long> getDirtyPages() {
        // 演示用，实际实现应该从缓冲池管理器获取
        Map<PageID, Long> result = new ConcurrentHashMap<>();

        // 这里应该遍历所有脏页，但由于没有直接访问的方法，我们创建一个模拟实现

        return result;
    }

    @Override
    public void recover() {
        lock.lock();
        try {
            // 如果有RecoveryManager，应该使用它来执行恢复
            if (txManager != null && pageManager != null) {
                RecoveryManager recoveryManager = new RecoveryManager(this, pageManager, txManager);
                recoveryManager.recover();
            } else {
                // 否则使用基本的恢复流程
                recoverBasic();
            }
        } finally {
            lock.unlock();
        }
    }

    // 基本恢复流程
    protected void recoverBasic() {
        // 恢复过程分为三个阶段：分析、重做、撤销

        // 1. 分析阶段：扫描日志，确定哪些事务需要重做，哪些需要撤销
        // 从检查点开始(如果有)，否则从头开始
        long scanPos = checkpointPosition > 0 ? checkpointPosition : LOG_HEADER_SIZE;

        // TODO: 实现分析阶段

        // 2. 重做阶段：重做所有日志操作(包括已提交和未提交的)
        // TODO: 实现重做阶段

        // 3. 撤销阶段：撤销未提交事务的操作
        // TODO: 实现撤销阶段

        // 恢复完成后，创建一个新的检查点
        checkpoint();
    }

    @Override
    public LogIterator iterator() {
        return new LogIteratorImpl(LOG_HEADER_SIZE);
    }

    // 返回从特定位置开始的日志迭代器
    public LogIterator iteratorFrom(long position) {
        return new LogIteratorImpl(position);
    }

    @Override
    public void close() {
        lock.lock();
        try {
            flush(); // 关闭前确保数据已写入
            try {
                logFile.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close log file", e);
            }
        } finally {
            lock.unlock();
        }
    }

    // 实现日志迭代器
    private class LogIteratorImpl implements LogIterator {
        private long currentPos;
        private boolean isRedoFlag;

        public LogIteratorImpl(long startPos) {
            this.currentPos = startPos;
        }

        @Override
        public boolean hasNext() {
            return currentPos < position;
        }

        @Override
        public byte[] next() {
            if(!hasNext()) {
                return null;
            }

            // 读取日志类型和大小
            try {
                ByteBuffer headerBuf = ByteBuffer.allocate(5); // type(1) + size(4)
                fc.read(headerBuf, currentPos);
                headerBuf.flip();

                byte type = headerBuf.get();
                isRedoFlag = (type == LOG_TYPE_REDO);

                int size = headerBuf.getInt();

                // 读取日志数据
                ByteBuffer dataBuf = ByteBuffer.allocate(size);
                fc.read(dataBuf, currentPos + 5);
                dataBuf.flip();

                byte[] data = new byte[size];
                dataBuf.get(data);

                // 更新位置
                currentPos += 5 + size;

                return data;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read next log", e);
            }
        }

        @Override
        public long position() {
            return currentPos;
        }

        @Override
        public boolean isRedo() {
            return isRedoFlag;
        }
    }
}
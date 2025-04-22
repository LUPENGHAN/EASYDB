package org.lupenghan.eazydb.log.Impl;


import lombok.extern.slf4j.Slf4j;
import org.lupenghan.eazydb.log.interfaces.LogManager;
import org.lupenghan.eazydb.log.models.LogPage;
import org.lupenghan.eazydb.log.models.LogRecord;
import org.lupenghan.eazydb.log.models.LogRecord1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
@Slf4j
public class LogManagerImpl implements LogManager {
    private RandomAccessFile raFile;
    private long nextLSN;       // 下一个要分配的日志序号（全局递增的LSN）
    private LogPage currentPage; // 当前正在写入的日志页

    // 构造函数：指定日志文件路径，初始化日志文件并日志状态
    public LogManagerImpl(String filePath) throws IOException {
        // 打开日志文件（不存在则创建）
        File file = new File(filePath);
        this.raFile = new RandomAccessFile(file, "rw");
        long fileLength = raFile.length();
        if (fileLength > 0) {
            // 如果日志文件长度不是整页大小倍数，说明上次写入时页面未完整写入，截断无效部分
            long remainder = fileLength % LogPage.PAGE_SIZE;
            if (remainder != 0) {
                raFile.setLength(fileLength - remainder);
                fileLength = raFile.length();
            }
            if (fileLength > 0) {
                // 读取最后一个页面的页头以确定最后的LSN
                long lastPageOffset = fileLength - LogPage.PAGE_SIZE;
                raFile.seek(lastPageOffset);
                byte[] lastPageData = new byte[LogPage.PAGE_SIZE];
                raFile.readFully(lastPageData);
                ByteBuffer headerBuf = ByteBuffer.wrap(lastPageData);
                long lastPageLSN = headerBuf.getLong(); // 页头中的 pageLSN
                /* 跳过 entryCount 字段4字节 */
                headerBuf.getInt();
                // 将下一个LSN设置为最后LSN+1，保证新日志LSN连续递增
                this.nextLSN = lastPageLSN + 1;
            } else {
                // 文件截断后变为空，说明没有完整页面
                this.nextLSN = 1;
            }
            // 移动文件指针到末尾，准备追加新日志页
            raFile.seek(fileLength);
        } else {
            // 日志文件为空，从LSN=1开始
            this.nextLSN = 1;
        }
        // 初始化当前页
        this.currentPage = new LogPage();
    }

    // 默认构造函数：使用默认日志文件名 "wal.log"
    public LogManagerImpl() throws IOException {
        this("wal.log");
    }

    @Override
    public synchronized void appendLog(LogRecord record) throws IOException {
        // 检查日志记录大小是否超过单页可用容量（4096 - 页头12字节）
        if (record.getTotalSize() > LogPage.PAGE_SIZE - 12) {
            throw new IllegalArgumentException("LogRecord is too large to fit in a single page");
        }
        // 当前页空间不足时，先刷盘当前页并换页
        if (!currentPage.hasSpaceFor(record)) {
            flushCurrentPage();
        }
        // 分配新的 LSN 并将记录追加到当前页
        record.setLsn(nextLSN++);
        currentPage.addRecord(record);
    }

    // 将当前页写入磁盘并开启一个新页
    private void flushCurrentPage() throws IOException {
        if (currentPage.getEntryCount() == 0) {
            // 当前页无记录则无需写盘
            return;
        }
        // 序列化当前页数据为4096字节
        byte[] pageData = currentPage.serialize();
        // 追加写入日志文件
        raFile.write(pageData);
        // 强制刷新操作系统缓冲区，保证数据持久化到磁盘
        raFile.getFD().sync();
        // 切换到新的空日志页
        currentPage = new LogPage();
    }

    @Override
    public synchronized List<LogRecord> loadAllLogs() throws IOException {
        // 先将尚未写入磁盘的最后一页刷盘，确保日志文件完整
        flushCurrentPage();
        // 从文件开始顺序读取所有日志页
        raFile.seek(0);
        List<LogRecord> allLogs = new ArrayList<>();
        long fileLength = raFile.length();
        long fullPages = fileLength / LogPage.PAGE_SIZE;
        byte[] pageData = new byte[LogPage.PAGE_SIZE];
        for (long i = 0; i < fullPages; i++) {
            raFile.readFully(pageData);
            LogPage page = LogPage.deserialize(pageData);
            allLogs.addAll(page.getRecords());
        }
        // 如存在未满一页的残留部分（未完全写入的页），将其忽略
        // 更新 nextLSN 为最后一条日志的下一个值，便于继续追加新的日志
        if (!allLogs.isEmpty()) {
            long lastLSN = allLogs.get(allLogs.size() - 1).getLsn();
            nextLSN = lastLSN + 1;
        }
        return allLogs;
    }
//    private static final String LOG_FILE = "transaction.log";
//    private static final int LOG_BUFFER_SIZE = 8192; // 8KB
//    private final File logFile;
//    private RandomAccessFile raf;
//    private FileChannel channel;
//    private final ByteBuffer buffer;
//    private final AtomicLong nextLSN;
//    private final Map<Long, List<LogRecord1>> transactionLogs;
//
//    private LogRecord1 latestCheckpoint;
//
//
//    public LogManagerImpl(String logDir) {
//        this.logFile = new File(logDir,LOG_FILE);
//        this.buffer = ByteBuffer.allocate(LOG_BUFFER_SIZE);
//        this.nextLSN = new AtomicLong(1);
//        this.transactionLogs = new HashMap<>();
//    }
//    @Override
//    public void init() {
//        try {
//            // 从文件中读取日志记录
//            readLogsFromFile();
//            System.out.println("LogManagerImpl.init(): 完成初始化，从文件读取了日志");
//        } catch (Exception e) {
//            log.error("Failed to initialize log manager", e);
//            throw new RuntimeException("Failed to initialize log manager", e);
//        }
//    }
//
//    @Override
//    public long writeRedoLog(long xid, int pageID, short offset, byte[] newData) {
//        return 0;
//    }
//
//    @Override
//    public long writeUndoLog(long xid, int operationType, byte[] undoData) {
//        return 0;
//    }
//
//    @Override
//    public LogRecord1 readLog(long lsn) {
//        return null;
//    }
//
//    @Override
//    public List<LogRecord1> getTransactionLogs(long xid) {
//        return List.of();
//    }
//
//    @Override
//    public void createCheckpoint() {
//
//    }
//
//    @Override
//    public List<Long> getActiveTransactions() {
//        return List.of();
//    }
//
//    @Override
//    public void recover() {
//
//    }
//
//    @Override
//    public void close() {
//
//    }
//
//    /**
//     * 确保FileChannel已初始化
//     */
//    private synchronized void ensureChannelInitialized() {
//        try {
//            if (channel == null) {
//                if (!logFile.exists()) {
//                    logFile.createNewFile();
//                }
//                this.raf = new RandomAccessFile(logFile, "rw");
//                this.channel = raf.getChannel();
//                System.out.println("LogManagerImpl.ensureChannelInitialized(): 初始化FileChannel");
//            }
//        } catch (IOException e) {
//            log.error("Failed to initialize channel", e);
//            throw new RuntimeException("Failed to initialize channel", e);
//        }
//    }
//    /**
//     * 从文件读取日志记录，不清空当前事务日志
//     */
//    private void readLogsFromFile() {
//        try {
//            // 确保channel已初始化
//            ensureChannelInitialized();
//
//            // 如果文件是空的，直接返回
//            if (channel.size() == 0) {
//                System.out.println("LogManagerImpl.readLogsFromFile(): 日志文件为空");
//                return;
//            }
//
//            System.out.println("LogManagerImpl.readLogsFromFile(): 开始读取日志文件，文件大小: " + channel.size());
//            channel.position(0);
//
//            while (true) {
//                // 读取LSN (8字节) 和 长度 (4字节)
//                buffer.clear();
//                buffer.limit(12); // 只读取LSN和长度字段
//
//                int bytesRead = channel.read(buffer);
//                if (bytesRead < 12) {
//                    // 如果读取不到完整的头部，说明文件已经结束
//                    System.out.println("LogManagerImpl.readLogsFromFile(): 读取文件结束，bytesRead=" + bytesRead);
//                    break;
//                }
//
//                buffer.flip();
//                long lsn = buffer.getLong();
//                int length = buffer.getInt();
//
//                System.out.println("LogManagerImpl.readLogsFromFile(): 读取到日志记录，LSN=" + lsn + ", length=" + length);
//
//                // 读取日志数据
//                byte[] data = new byte[length];
//                buffer.clear();
//
//                ByteBuffer dataBuffer = ByteBuffer.wrap(data);
//                int dataRead = channel.read(dataBuffer);
//                if (dataRead < length) {
//                    System.out.println("LogManagerImpl.readLogsFromFile(): 警告，日志数据不完整, dataRead=" + dataRead + ", length=" + length);
//                    break;
//                }
//
//                LogRecord1 logRecord = LogRecord1.deserialize(data);
//                transactionLogs.computeIfAbsent(logRecord.getXid(), k -> new ArrayList<>())
//                        .add(logRecord);
//
//                System.out.println("LogManagerImpl.readLogsFromFile(): 添加日志记录，事务ID=" + logRecord.getXid() +
//                        ", 日志类型=" + logRecord.getLogType());
//
//                if (logRecord.getLogType() == LogRecord1.TYPE_CHECKPOINT) {
//                    latestCheckpoint = logRecord;
//                    System.out.println("LogManagerImpl.readLogsFromFile(): 发现检查点日志");
//                }
//
//                nextLSN.set(lsn + 1);
//            }
//
//            System.out.println("LogManagerImpl.readLogsFromFile(): 日志文件读取完成，事务数: " + transactionLogs.size());
//            for (Map.Entry<Long, List<LogRecord1>> entry : transactionLogs.entrySet()) {
//                System.out.println("LogManagerImpl.readLogsFromFile(): 事务ID=" + entry.getKey() +
//                        ", 日志记录数=" + entry.getValue().size());
//            }
//
//        } catch (IOException e) {
//            log.error("Failed to read logs from file", e);
//            throw new RuntimeException("Failed to read logs from file", e);
//        }
//    }

}

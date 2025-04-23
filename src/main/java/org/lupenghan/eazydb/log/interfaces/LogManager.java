package org.lupenghan.eazydb.log.interfaces;

import org.lupenghan.eazydb.log.models.LogRecord;

import java.io.IOException;
import java.util.List;

public interface LogManager {

    /**
     * 追加一条新的日志记录到日志中。如果当前页空间不足，则会先将当前页写入磁盘并开启新页。
     * @param record 要追加的日志记录对象（调用时将由日志管理器分配LSN）
     * @throws IOException 当写入磁盘发生错误时抛出异常
     */
    void appendLog(LogRecord record) throws IOException;

    void flush() throws IOException;
    /**
     * 从磁盘加载所有日志页并提取其中的日志记录列表，用于系统恢复。
     * 顺序读取磁盘上所有日志页，反序列化得到日志记录。
     * @return 按照写入顺序包含所有日志记录的列表
     * @throws IOException 当从磁盘读取发生错误时抛出异常
     */
    List<LogRecord> loadAllLogs() throws IOException;
//    void init();

//
//    long writeRedoLog(long xid, int pageID, short offset, byte[] newData);
//
//
//    long writeUndoLog(long xid, int operationType, byte[] undoData);
//
//
//    LogRecord1 readLog(long lsn);
//
//
//    List<LogRecord1> getTransactionLogs(long xid);
//
//
//    void createCheckpoint();
//
//    //获取全部未完成
//    List<Long> getActiveTransactions();
//
//
//    void recover();
//
//
//    void close();
}

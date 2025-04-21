package org.lupenghan.eazydb.log.interfaces;

import org.lupenghan.eazydb.log.models.LogRecord;

import java.util.List;

public interface LogManager {
    /**
     * 初始化日志管理器
     */
    void init();

    /**
     * 写入重做日志
     * @param xid 事务ID
     * @param pageID 页面ID
     * @param offset 偏移量
     * @param newData 新数据
     * @return 日志序列号
     */
    long writeRedoLog(long xid, int pageID, short offset, byte[] newData);

    /**
     * 写入撤销日志
     * @param xid 事务ID
     * @param operationType 操作类型
     * @param undoData 撤销数据
     * @return 日志序列号
     */
    long writeUndoLog(long xid, int operationType, byte[] undoData);

    /**
     * 读取日志记录
     * @param lsn 日志序列号
     * @return 日志记录
     */
    LogRecord readLog(long lsn);

    /**
     * 获取指定事务的所有日志记录
     * @param xid 事务ID
     * @return 日志记录列表
     */
    List<LogRecord> getTransactionLogs(long xid);

    /**
     * 创建检查点
     */
    void createCheckpoint();

    /**
     * 获取所有未完成的事务ID
     * @return 未完成事务ID列表
     */
    List<Long> getActiveTransactions();

    /**
     * 恢复数据库
     */
    void recover();

    /**
     * 关闭日志管理器
     */
    void close();
}

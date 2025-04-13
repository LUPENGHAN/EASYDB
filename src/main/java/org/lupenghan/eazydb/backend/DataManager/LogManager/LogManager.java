package org.lupenghan.eazydb.backend.DataManager.LogManager;

public interface LogManager {
    // 将日志记录追加到日志文件，返回日志序号(LSN)
    long appendRedoLog(long xid, int pageID, short offset, byte[] oldData, byte[] newData);

    // 追加Undo日志记录
    long appendUndoLog(long xid, int operationType, byte[] undoData);

    // 根据LSN读取一条重做日志
    byte[] readRedoLog(long lsn);

    // 根据LSN读取一条撤销日志
    byte[] readUndoLog(long lsn);

    // 强制将日志缓冲区内容写入磁盘
    void flush();

    // 创建检查点
    void checkpoint();

    // 系统启动时进行恢复操作
    void recover();

    // 关闭日志管理器
    void close();

    // 迭代器接口用于遍历日志
    interface LogIterator {
        boolean hasNext();
        byte[] next();
        long position();
        boolean isRedo(); // 判断当前日志是redo还是undo
    }

    // 获取日志迭代器
    LogIterator iterator();
}
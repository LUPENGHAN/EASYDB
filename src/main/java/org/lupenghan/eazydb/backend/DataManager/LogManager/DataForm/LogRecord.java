package org.lupenghan.eazydb.backend.DataManager.LogManager.DataForm;

import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
public class LogRecord {
    // 日志类型常量
    public static final byte TYPE_REDO = 0;
    public static final byte TYPE_UNDO = 1;
    public static final byte TYPE_CHECKPOINT = 2;
    public static final byte TYPE_COMPENSATION = 3;
    public static final byte TYPE_END_CHECKPOINT = 4;
    public static final byte TYPE_BEGIN_CHECKPOINT = 5;

    // UNDO操作类型常量
    public static final int UNDO_INSERT = 0;
    public static final int UNDO_DELETE = 1;
    public static final int UNDO_UPDATE = 2;

    private byte logType;     // 日志类型
    private long xid;         // 事务ID

    // REDO日志特有字段
    private int pageID;       // 页面ID
    private short offset;     // 页内偏移量
    private byte[] oldData;   // 修改前数据
    private byte[] newData;   // 修改后数据

    // UNDO日志特有字段
    private int operationType; // 操作类型
    private byte[] undoData;   // 撤销数据

    // 空构造函数
    public LogRecord() {
        this.oldData = new byte[0];
        this.newData = new byte[0];
        this.undoData = new byte[0];
    }

    // REDO日志构造函数
    public static LogRecord createRedoLog(long xid, int pageID, short offset, byte[] oldData, byte[] newData) {
        LogRecord log = new LogRecord();
        log.logType = TYPE_REDO;
        log.xid = xid;
        log.pageID = pageID;
        log.offset = offset;
        log.oldData = oldData != null ? oldData : new byte[0];
        log.newData = newData != null ? newData : new byte[0];
        return log;
    }

    // UNDO日志构造函数
    public static LogRecord createUndoLog(long xid, int operationType, byte[] undoData) {
        LogRecord log = new LogRecord();
        log.logType = TYPE_UNDO;
        log.xid = xid;
        log.operationType = operationType;
        log.undoData = undoData != null ? undoData : new byte[0];
        return log;
    }

    // 序列化方法
    public byte[] serialize() {
        if (logType == TYPE_REDO) {
            return serializeRedoLog();
        } else if (logType == TYPE_UNDO) {
            return serializeUndoLog();
        }
        return new byte[0];
    }

    private byte[] serializeRedoLog() {
        int length = 8 + 4 + 2 + 4 + 4 + oldData.length + newData.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);

        buffer.putLong(xid);
        buffer.putInt(pageID);
        buffer.putShort(offset);
        buffer.putInt(oldData.length);
        buffer.putInt(newData.length);
        buffer.put(oldData);
        buffer.put(newData);

        return buffer.array();
    }

    private byte[] serializeUndoLog() {
        int length = 8 + 4 + 4 + undoData.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);

        buffer.putLong(xid);
        buffer.putInt(operationType);
        buffer.putInt(undoData.length);
        buffer.put(undoData);

        return buffer.array();
    }

    // 反序列化方法
    public static LogRecord deserialize(byte logType, byte[] data) {
        if (logType == TYPE_REDO) {
            return deserializeRedoLog(data);
        } else if (logType == TYPE_UNDO) {
            return deserializeUndoLog(data);
        }
        return null;
    }

    private static LogRecord deserializeRedoLog(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        LogRecord log = new LogRecord();
        log.logType = TYPE_REDO;
        log.xid = buffer.getLong();
        log.pageID = buffer.getInt();
        log.offset = buffer.getShort();

        int oldDataLen = buffer.getInt();
        int newDataLen = buffer.getInt();

        log.oldData = new byte[oldDataLen];
        log.newData = new byte[newDataLen];

        buffer.get(log.oldData);
        buffer.get(log.newData);

        return log;
    }

    private static LogRecord deserializeUndoLog(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        LogRecord log = new LogRecord();
        log.logType = TYPE_UNDO;
        log.xid = buffer.getLong();
        log.operationType = buffer.getInt();

        int undoDataLen = buffer.getInt();
        log.undoData = new byte[undoDataLen];
        buffer.get(log.undoData);

        return log;
    }
}
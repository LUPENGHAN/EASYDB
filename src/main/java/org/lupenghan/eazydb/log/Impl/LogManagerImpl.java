package org.lupenghan.eazydb.log.Impl;


import org.lupenghan.eazydb.log.interfaces.LogManager;
import org.lupenghan.eazydb.log.models.LogRecord;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class LogManagerImpl implements LogManager {
    private static final String LOG_FILE = "transaction.log";
    private static final int LOG_BUFFER_SIZE = 8192; // 8KB
    private final File logFile;
    private RandomAccessFile raf;
    private FileChannel channel;
    private final ByteBuffer buffer;
    private final AtomicLong nextLSN;
    private final Map<Long, List<LogRecord>> transactionLogs;

    public LogManagerImpl(String logDir) {
        this.logFile = new File(logDir,LOG_FILE);
        this.buffer = ByteBuffer.allocate(LOG_BUFFER_SIZE);
        this.nextLSN = new AtomicLong(1);
        this.transactionLogs = new HashMap<>();
    }
    @Override
    public void init() {

    }

    @Override
    public long writeRedoLog(long xid, int pageID, short offset, byte[] newData) {
        return 0;
    }

    @Override
    public long writeUndoLog(long xid, int operationType, byte[] undoData) {
        return 0;
    }

    @Override
    public LogRecord readLog(long lsn) {
        return null;
    }

    @Override
    public List<LogRecord> getTransactionLogs(long xid) {
        return List.of();
    }

    @Override
    public void createCheckpoint() {

    }

    @Override
    public List<Long> getActiveTransactions() {
        return List.of();
    }

    @Override
    public void recover() {

    }

    @Override
    public void close() {

    }
}

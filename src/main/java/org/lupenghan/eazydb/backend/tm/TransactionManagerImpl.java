package org.lupenghan.eazydb.backend.tm;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

public class TransactionManagerImpl implements TransactionManager {
    //固定数据
    private static final byte ACTIVE = 1;
    private static final byte COMMITTED = 2;
    private static final byte ABORT = 3;


    // 事务文件头大小（8字节存储XID计数器）
    private static final int XID_HEADER_LENGTH = 8;
    // 每个事务状态占用的字节数
    private static final int XID_FIELD_SIZE = 1;
    // 事务文件
    private RandomAccessFile xidFile;
    // 当前最大事务ID
    private long xidCounter;
    // 事务文件对应的通道
    private FileChannel fc;

    // 缓存已提交事务的位图（优化查询）
    private Set<Long> cachedCommittedTransactions;

    // 实现初始化功能
    public TransactionManagerImpl(String path) throws Exception {
        File file = new File(path + "/xid.txn");
        boolean isNewFile = !file.exists();
        //确保目录存在
        xidFile = new RandomAccessFile(file, "rw");
        fc = xidFile.getChannel();
        if (isNewFile) {
            // 新文件，初始化XID为1（0是保留的无效事务ID）
            ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
            buffer.putLong(1);
            buffer.flip();
            xidCounter = 1;
        } else {
            // 已有文件，读取现有XID计数器
            ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
            fc.read(buf, 0);
            buf.flip();
            xidCounter = buf.getLong();
        }
        cachedCommittedTransactions = new HashSet<>();
        if (!isNewFile) {
            recoverCommittedTransactions();
        }
    }


    @Override
    public long begin() {
        long xid = xidCounter++;
        updateXID(xid,ACTIVE);
        // 更新XID计数器到文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
        buf.putLong(xidCounter);
        buf.flip();
        try {
            fc.write(buf, 0);
            fc.force(false); // 确保持久化
        } catch (IOException e) {
            throw new RuntimeException("Failed to begin transaction", e);
        }

        return xid;    }


    @Override
    public void commit(long xid) {
        updateXID(xid,COMMITTED);
        cachedCommittedTransactions.add(xid);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid,ABORT);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == 0) return false;

        return checkXIDState(xid,ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == 0) return true; // XID 0视为已提交

        // 先查缓存，提高性能
        if(cachedCommittedTransactions.contains(xid)) {
            return true;
        }

        return checkXIDState(xid, COMMITTED);
    }

    @Override
    public boolean isAbort(long xid) {
        if(xid == 0) return false; // XID 0不会是中止状态
        return checkXIDState(xid, ABORT);
    }
    @Override
    public void close( ) {
        try {
            xidFile.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close transaction file", e);
        }
    }
    //辅助方法
    private void updateXID(long xid, byte status) {
        long offset = XID_HEADER_LENGTH + (xid-1) * XID_FIELD_SIZE;
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        buffer.put(status);
        buffer.flip();
        try{
            fc.write(buffer,offset);
            fc.force(false);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update XID(transaction state)", e);
        }
    }
    private void recoverCommittedTransactions() throws IOException{
        long fileSize = fc.size();
        long xidCount = (fileSize - XID_HEADER_LENGTH) / XID_FIELD_SIZE;

        //批量读取状态提高性能
        ByteBuffer buffer = ByteBuffer.allocate((int)fileSize - XID_HEADER_LENGTH);
        fc.read(buffer,XID_HEADER_LENGTH);
        buffer.flip();
        for(long i=0; i<xidCount; i++){
            byte status = buffer.get();
            if(status == COMMITTED){
                cachedCommittedTransactions.add(i+1);
            }
        }
    }
    // 检查事务状态
    private boolean checkXIDState(long xid, byte state) {
        long offset = XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;

        // 如果事务ID超出文件范围，则状态一定是活跃的
        try {
            if(offset >= fc.size()) {
                return state == ACTIVE;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check file size", e);
        }

        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        try {
            fc.read(buf, offset);
            buf.flip();
            return buf.get() == state;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read transaction state", e);
        }
    }
    public synchronized void checkpoint() {
        try {
            // 强制将所有更改写入磁盘
            fc.force(true);

            // 可以在这里进行额外的检查点逻辑，如清理过时的事务记录
            // 在更复杂的实现中，可能需要协调其他管理器（如日志管理器）
        } catch (IOException e) {
            throw new RuntimeException("Failed to create checkpoint", e);
        }
    }
}

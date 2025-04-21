package org.lupenghan.eazydb.transaction.Impl;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.lupenghan.eazydb.lock.interfaces.LockManager;
import org.lupenghan.eazydb.lock.models.Lock;
import org.lupenghan.eazydb.lock.models.LockType;
import org.lupenghan.eazydb.log.interfaces.LogManager;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.transaction.interfaces.TransactionManager;
import org.lupenghan.eazydb.transaction.models.TransactionStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
@AllArgsConstructor
@Slf4j
public class TransactionManagerImpl implements TransactionManager {
    //导入其他管理工具
    private final LogManager logManager;
    private final LockManager lockManager;
    private final AtomicLong nextXid;
    private final Map<Long, TransactionStatus> transactionStatus;
    private final Map<Long, Map<Page, Lock>> transactionLocks;
    private final Map<Long, List<Page>> modifiedPagesMap;
    private final Map<Long, List<Record>> modifiedRecordsMap;
    public TransactionManagerImpl(LogManager logManager, LockManager lockManager) {
        this.logManager = logManager;
        this.lockManager = lockManager;
        this.nextXid = new AtomicLong(1);
        this.transactionStatus = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.modifiedPagesMap = new ConcurrentHashMap<>();
        this.modifiedRecordsMap = new ConcurrentHashMap<>();
    }

    @Override
    public long begin() {
        long xid = nextXid.getAndIncrement();
        transactionStatus.put(xid, TransactionStatus.ACTIVE);
        transactionLocks.put(xid, new ConcurrentHashMap<>());
        modifiedPagesMap.put(xid, new ArrayList<>());
        modifiedRecordsMap.put(xid, new ArrayList<>());
        logManager.writeRedoLog(xid, 0, (short) 0, new byte[0]); // 写入开始事务日志
        return xid;
    }

    @Override
    public void commit(long xid) {
        if (!transactionStatus.containsKey(xid)) {
            throw new IllegalArgumentException("Transaction " + xid + " does not exist");
        }

        try {
//            // 释放所有锁
//            lockManager.releaseAllLocks(xid);
//            transactionLocks.remove(xid);
//            modifiedPagesMap.remove(xid);
//            modifiedRecordsMap.remove(xid);

            // 写入提交日志
            logManager.writeRedoLog(xid, 0, (short) 0, new byte[0]);

            // 更新事务状态
            transactionStatus.put(xid, TransactionStatus.COMMITTED);
        } catch (Exception e) {
            log.error("Failed to commit transaction {}", xid, e);
            rollback(xid);
            throw new RuntimeException("Failed to commit transaction " + xid, e);
        }
    }

    @Override
    public void rollback(long xid) {
        if (!transactionStatus.containsKey(xid)) {
            throw new IllegalArgumentException("Transaction " + xid + " does not exist");
        }

        try {
//            // 释放所有锁
//            lockManager.releaseAllLocks(xid);
//            transactionLocks.remove(xid);
//            modifiedPagesMap.remove(xid);
//            modifiedRecordsMap.remove(xid);

            // 写入回滚日志
            logManager.writeUndoLog(xid, 0, new byte[0]);

            // 更新事务状态
            transactionStatus.put(xid, TransactionStatus.ABORTED);
        } catch (Exception e) {
            log.error("Failed to rollback transaction {}", xid, e);
            throw new RuntimeException("Failed to rollback transaction " + xid, e);
        }
    }

    @Override
    public TransactionStatus getTransactionsStatus(long xid) {
        return transactionStatus.getOrDefault(xid, TransactionStatus.ABORTED);
    }

    //锁相关
    @Override
    public List<Page> getModifiedPages(long TransactionID) {
        return List.of();
    }

    @Override
    public List<Record> getModifiedRecords(long transactionId) {
        return List.of();
    }

    @Override
    public Lock acquireLock(long transactionId, Page page, LockType lockType) {
        return null;
    }

    @Override
    public void releaseLock(long transactionId, Page page) {

    }

    @Override
    public boolean holdsLock(long transactionId, Page page) {
        return false;
    }
}

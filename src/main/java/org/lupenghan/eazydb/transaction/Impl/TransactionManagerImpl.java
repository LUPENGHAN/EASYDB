package org.lupenghan.eazydb.transaction.Impl;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lupenghan.eazydb.lock.interfaces.LockManager;
import org.lupenghan.eazydb.lock.models.Lock;
import org.lupenghan.eazydb.lock.models.LockType;
import org.lupenghan.eazydb.log.interfaces.LogManager;
import org.lupenghan.eazydb.log.models.LogRecord;
import org.lupenghan.eazydb.page.interfaces.PageManager;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.record.interfaces.RecordManager;
import org.lupenghan.eazydb.transaction.interfaces.TransactionManager;
import org.lupenghan.eazydb.transaction.models.TransactionStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class TransactionManagerImpl implements TransactionManager {
    //导入其他管理工具
    private final LogManager logManager;
    private final LockManager lockManager;
    private final PageManager pageManager;
    private final AtomicLong nextXid;
    private final Map<Long, TransactionStatus> transactionStatus;
    private final Map<Long, Map<Page, Lock>> transactionLocks;
    private final Map<Long, List<Page>> modifiedPagesMap;
    private final Map<Long, List<Record>> modifiedRecordsMap;
    public TransactionManagerImpl(LogManager logManager, LockManager lockManager, PageManager pageManager ) {
        this.logManager = logManager;
        this.lockManager = lockManager;
        this.pageManager = pageManager;
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
        return xid;
    }

    @Override
    public void commit(long xid) throws IOException {
        if (!transactionStatus.containsKey(xid)) {
            throw new IllegalArgumentException("Transaction " + xid + " does not exist");
        }

        try {
            // 写入提交日志
            logManager.flush();
            // 更新事务状态
            transactionStatus.put(xid, TransactionStatus.COMMITTED);
            
            // 释放所有锁
            lockManager.releaseAllLocks(xid);
            transactionLocks.remove(xid);
            modifiedPagesMap.remove(xid);
            modifiedRecordsMap.remove(xid);
            
            log.info("事务 {} 已成功提交", xid);
        } catch (Exception e) {
            log.error("Failed to commit transaction {}", xid, e);
            rollback(xid);
            throw new RuntimeException("Failed to commit transaction " + xid, e);
        }
    }

    public void rollback(long xid) throws IOException {
        if (!transactionStatus.containsKey(xid)) {
            throw new IllegalArgumentException("Transaction " + xid + " does not exist");
        }

        // 委托给RecordManager执行具体的回滚操作
//        recordManager.rollbackTransaction(xid);

        // 更新事务状态和清理
        transactionStatus.put(xid, TransactionStatus.ABORTED);
        lockManager.releaseAllLocks(xid);
        transactionLocks.remove(xid);
        modifiedPagesMap.remove(xid);
        modifiedRecordsMap.remove(xid);

        log.info("事务 {} 已回滚", xid);
    }
    public List<LogRecord> getUndoLogs(long xid) throws IOException {
        List<LogRecord> logs = logManager.loadAllLogs();
        return logs.stream()
                .filter(l -> l.getXid() == xid && l.getLogType() == LogRecord.TYPE_UNDO)
                .collect(Collectors.toList());
    }
    @Override
    public TransactionStatus getTransactionsStatus(long xid) {
        return transactionStatus.getOrDefault(xid, TransactionStatus.ABORTED);
    }

    //锁相关
    @Override
    public List<Page> getModifiedPages(long TransactionID) {
        return new ArrayList<>(modifiedPagesMap.getOrDefault(TransactionID, new ArrayList<>()));

    }

    @Override
    public List<Record> getModifiedRecords(long transactionId) {
        return new ArrayList<>(modifiedRecordsMap.getOrDefault(transactionId, new ArrayList<>()));
    }

    @Override
    public Lock acquireLock(long transactionId, Page page, LockType lockType) {
        int pageId = page.getHeader().getPageId();
        int slotId = -1; // 默认为页级锁
        
        // 获取锁
        Lock lock = lockManager.acquireLock(transactionId, lockType, pageId, slotId);
        
        if (lock != null) {
            // 锁获取成功，记录在事务锁映射中
            Map<Page, Lock> locks = transactionLocks.computeIfAbsent(transactionId, k -> new ConcurrentHashMap<>());
            locks.put(page, lock);
            
            // 如果是写操作，将页面添加到修改页面列表中
            if (lockType == LockType.EXCLUSIVE_LOCK) {
                modifiedPagesMap.computeIfAbsent(transactionId, k -> new ArrayList<>()).add(page);
            }
        } else {
            log.warn("事务 {} 无法获取页 {} 的 {} 锁", transactionId, pageId, lockType);
        }
        
        return lock;
    }

    @Override
    public void releaseLock(long transactionId, Page page) {
        Map<Page, Lock> locks = transactionLocks.get(transactionId);
        if (locks != null) {
            Lock lock = locks.remove(page);
            if (lock != null) {
                lockManager.releaseLock(lock);
            }
        }
    }

    @Override
    public boolean holdsLock(long transactionId, Page page) {
        Map<Page, Lock> locks = transactionLocks.get(transactionId);
        return locks != null && locks.containsKey(page);    }

    @Override
    public LockManager getLockManager() {
        return this.lockManager;
    }
}

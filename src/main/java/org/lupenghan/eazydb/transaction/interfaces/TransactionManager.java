package org.lupenghan.eazydb.transaction.interfaces;

import org.lupenghan.eazydb.lock.models.Lock;
import org.lupenghan.eazydb.lock.models.LockType;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.transaction.models.TransactionStatus;

import java.io.IOException;
import java.util.List;

public interface TransactionManager {
    long begin();
    void commit(long xid) throws IOException;
    void rollback(long xid) throws IOException;
    //获得事务状态
    TransactionStatus getTransactionsStatus(long TransactionsId);
    // 获得哪些内容修改了, 用于进行数据恢复
    List<Page> getModifiedPages (long TransactionID);
    List<Record> getModifiedRecords(long transactionId);

    //锁相关
    /**
     * 获取锁
     * @param transactionId 事务ID
     * @param page 页面
     * @param lockType 锁类型
     * @return 锁对象
     */
    Lock acquireLock(long transactionId, Page page, LockType lockType);

    /**
     * 释放锁
     * @param transactionId 事务ID
     * @param page 页面
     */
    void releaseLock(long transactionId, Page page);

    /**
     * 检查是否持有锁
     * @param transactionId 事务ID
     * @param page 页面
     * @return 是否持有锁
     */
    boolean holdsLock(long transactionId, Page page);

}

package org.lupenghan.eazydb.lock.interfaces;

import org.lupenghan.eazydb.lock.models.Lock;
import org.lupenghan.eazydb.lock.models.LockType;

import java.util.List;

public interface LockManager {
    /**
     * 获取锁
     * @param xid 事务Id
     * @param type 锁类型
     * @param pageId 页面Id
     * @param slotId 槽位Id
     * @return 锁对象
     */
    Lock acquireLock(long xid, LockType type, int pageId, int slotId);
    /**
     * 释放锁
     * @param lock 要释放的锁
     */
    void releaseLock(Lock lock);

    /**
     * 释放事务的所有锁
     * @param xid 事务ID
     */
    void releaseAllLocks(long xid);

    /**
     * 检查是否存在死锁
     * @param xid 事务ID
     * @return 是否存在死锁
     */
    boolean hasDeadlock(long xid);

    /**
     * 获取事务持有的所有锁
     * @param xid 事务ID
     * @return 锁列表
     */
    List<Lock> getLocksByXid(long xid);

    /**
     * 获取页面上的所有锁
     * @param pageId 页面ID
     * @return 锁列表
     */
    List<Lock> getLocksByPage(int pageId);

    /**
     * 获取记录上的所有锁
     * @param pageId 页面ID
     * @param slotId 槽位ID
     * @return 锁列表
     */
    List<Lock> getLocksByRecord(int pageId, int slotId);

    /**
     * 升级锁
     * @param lock 要升级的锁
     * @return 升级后的锁
     */
    Lock upgradeLock(Lock lock);
}

package org.lupenghan.eazydb.lock.models;

import lombok.Data;

import java.util.concurrent.locks.ReentrantReadWriteLock;
@Data
public class Lock {
    private final long xid;           // 事务ID
    private final LockType type;      // 锁类型
    private final int pageId;         // 页面ID
    private final int slotId;         // 槽位ID
    private final long timestamp;     // 加锁时间戳
    private final ReentrantReadWriteLock lock; // 实际的锁对象

    public Lock(long xid, LockType type, int pageId, int slotId) {
        this.xid = xid;
        this.type = type;
        this.pageId = pageId;
        this.slotId = slotId;
        this.timestamp = System.currentTimeMillis();
        this.lock = new ReentrantReadWriteLock();
    }

    public void lockRead() {lock.readLock().lock();}
    public void lockWrite() {lock.writeLock().lock();}
    public void unlockRead() {lock.readLock().unlock();}
    public void unlockWrite() {lock.writeLock().unlock();}
    // 是否可以升级为排他锁
    public boolean canUpgrade() {return type == LockType.SHARED_LOCK && lock.getReadHoldCount() == 1;}
    public void upgrade() {
        if (!canUpgrade()) {
            throw new IllegalStateException("Cannot upgrade lock");
        }
        lock.readLock().unlock();
        lock.writeLock().lock();
    }
}

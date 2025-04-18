package org.lupenghan.eazydb.backend.examples;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordVersion;
import org.lupenghan.eazydb.backend.TransactionManager.*;

/**
 * MVCC并发控制示例
 */
public class MVCCConcurrencyExample {
    public static void main(String[] args) throws Exception {
        // 创建事务管理器
        EnhancedTransactionManagerImpl txManager = new EnhancedTransactionManagerImpl("testdb");

        // 创建并发控制管理器
        ConcurrencyControlManager ccManager = new ConcurrencyControlManager(txManager);

        // 模拟记录ID和版本
        RecordID recordID = new RecordID(null, 1);

        // 事务1：读取记录
        long xid1 = txManager.begin();
        txManager.setIsolationLevel(xid1, IsolationLevel.REPEATABLE_READ);
        System.out.println("事务1开始 (xid=" + xid1 + ")");

        // 获取读锁（可能是虚拟的，由并发控制策略决定）
        if (ccManager.acquireReadLock(xid1, recordID)) {
            // 创建ReadView
            ReadView readView1 = txManager.createReadView(xid1);

            // 模拟读取记录
            System.out.println("事务1读取记录：成功 (使用ReadView: " + readView1.getReadTS() + ")");
        }

        // 事务2：修改记录
        long xid2 = txManager.begin();
        txManager.setIsolationLevel(xid2, IsolationLevel.READ_COMMITTED);
        System.out.println("事务2开始 (xid=" + xid2 + ")");

        // 获取写锁
        if (ccManager.acquireWriteLock(xid2, recordID)) {
            // 模拟修改记
            // 模拟修改记录
            System.out.println("事务2修改记录：成功");

            // 提交事务2
            txManager.commit(xid2);
            System.out.println("事务2提交");
        } else {
            System.out.println("事务2无法获取写锁，中止");
            txManager.abort(xid2);
        }

        // 事务3：读取已提交的修改
        long xid3 = txManager.begin();
        txManager.setIsolationLevel(xid3, IsolationLevel.READ_COMMITTED);
        System.out.println("事务3开始 (xid=" + xid3 + ")");

        // 获取读锁
        if (ccManager.acquireReadLock(xid3, recordID)) {
            // 创建ReadView（READ_COMMITTED每次读取创建新的ReadView）
            ReadView readView3 = txManager.createReadView(xid3);

            // 模拟读取记录
            System.out.println("事务3读取记录：成功 (使用ReadView: " + readView3.getReadTS() + ")");
            System.out.println("事务3可以看到事务2的修改");
        }

        // 事务1再次读取
        if (ccManager.acquireReadLock(xid1, recordID)) {
            // 重用之前的ReadView（REPEATABLE_READ隔离级别）
            ReadView readView1 = txManager.createReadView(xid1);

            // 模拟读取记录
            System.out.println("事务1再次读取记录：成功 (使用ReadView: " + readView1.getReadTS() + ")");
            System.out.println("事务1看不到事务2的修改（可重复读隔离级别）");
        }

        // 事务4：尝试修改已被修改的记录（乐观并发控制冲突）
        long xid4 = txManager.begin();
        System.out.println("事务4开始 (xid=" + xid4 + ")");

        // 获取写锁
        if (ccManager.acquireWriteLock(xid4, recordID)) {
            // 模拟读取记录版本
            long expectedVersion = 1;
            long actualVersion = 2; // 事务2已修改

            // 检查冲突
            if (ccManager.checkOptimisticConflict(xid4, recordID, expectedVersion, actualVersion)) {
                System.out.println("事务4检测到乐观并发冲突，需要回滚");
                txManager.abort(xid4);
            } else {
                // 模拟修改记录
                System.out.println("事务4修改记录：成功");
                txManager.commit(xid4);
            }
        }

        // 事务5：使用悲观锁控制
        long xid5 = txManager.begin();
        txManager.setIsolationLevel(xid5, IsolationLevel.SERIALIZABLE);
        System.out.println("事务5开始 (xid=" + xid5 + ")");

        // 强制使用悲观锁
        ccManager.recordConflict(recordID);

        // 获取写锁
        if (txManager.acquireExclusiveLock(xid5, recordID)) {
            // 模拟修改记录
            System.out.println("事务5修改记录：成功（使用悲观锁）");

            // 提交事务5
            txManager.commit(xid5);
            System.out.println("事务5提交");
        } else {
            System.out.println("事务5无法获取写锁，中止");
            txManager.abort(xid5);
        }

        // 提交或中止剩余事务
        txManager.commit(xid1);
        System.out.println("事务1提交");

        txManager.commit(xid3);
        System.out.println("事务3提交");

        // 关闭事务管理器
        txManager.close();
        System.out.println("示例完成");
    }
}
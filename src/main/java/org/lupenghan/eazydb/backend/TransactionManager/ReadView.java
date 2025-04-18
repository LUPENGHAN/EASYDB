package org.lupenghan.eazydb.backend.TransactionManager;

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

/**
 * 事务读视图，用于MVCC并发控制中的可见性判断
 */
@Getter
public class ReadView {
    // 创建该ReadView的事务ID
    private final long txID;

    // 创建ReadView时的系统时间戳
    private final long readTS;

    // 创建ReadView时的活跃事务列表
    private final Set<Long> activeTransactions;

    // 创建ReadView时的最小活跃事务ID
    private final long minActiveXID;

    // 隔离级别
    private final IsolationLevel isolationLevel;

    /**
     * 创建ReadView
     * @param txID 事务ID
     * @param readTS 读时间戳
     * @param activeTransactions 活跃事务集合
     * @param isolationLevel 隔离级别
     */
    public ReadView(long txID, long readTS, Set<Long> activeTransactions, IsolationLevel isolationLevel) {
        this.txID = txID;
        this.readTS = readTS;
        this.activeTransactions = new HashSet<>(activeTransactions);
        this.isolationLevel = isolationLevel;

        // 计算最小活跃事务ID
        long minXID = Long.MAX_VALUE;
        for (long xid : activeTransactions) {
            if (xid < minXID) {
                minXID = xid;
            }
        }
        this.minActiveXID = minXID == Long.MAX_VALUE ? 0 : minXID;
    }

    /**
     * 检查一个版本是否对当前ReadView可见
     * @param xid 创建版本的事务ID
     * @param beginTS 版本的开始时间戳
     * @param endTS 版本的结束时间戳
     * @return 是否可见
     */
    public boolean isVisible(long xid, long beginTS, long endTS) {
        // 1. 如果是当前事务创建的版本，对当前事务可见
        if (xid == txID) {
            return true;
        }

        // 2. 版本的beginTS必须小于等于readTS（创建版本的事务必须在readTS之前开始）
        if (beginTS > readTS) {
            return false;
        }

        // 3. 根据隔离级别进行不同的可见性判断
        switch (isolationLevel) {
            case READ_UNCOMMITTED:
                // 读未提交：允许读取未提交的数据
                return true;

            case READ_COMMITTED:
            case REPEATABLE_READ:
            case SERIALIZABLE:
                // 其他隔离级别：版本创建事务不能是活跃的（已提交）
                // 除非是创建该版本的事务就是当前事务
                if (activeTransactions.contains(xid) && xid != txID) {
                    return false;
                }

                // 4. 版本必须是在当前事务开始前未被删除的（endTS > readTS）
                // 或者版本是由活跃事务删除的（这种情况下我们看不到删除）
                return endTS > readTS || activeTransactions.contains(endTS);

            default:
                return false;
        }
    }
}
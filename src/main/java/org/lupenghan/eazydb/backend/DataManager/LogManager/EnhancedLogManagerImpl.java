package org.lupenghan.eazydb.backend.DataManager.LogManager;

import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

/**
 * LogManagerImpl增强版checkpoint方法
 */
public class EnhancedLogManagerImpl extends LogManagerImpl {
    private static final Logger LOGGER = Logger.getLogger(EnhancedLogManagerImpl.class.getName());

    private final CheckpointManager checkpointManager;
    private final TransactionManager txManager;
    private final PageManager pageManager;


    /**
     * 创建增强版日志管理器
     * @param path 数据库路径
     * @param txManager 事务管理器
     * @param pageManager 页面管理器
     * @throws Exception 如果创建失败
     */
    public EnhancedLogManagerImpl(String path, TransactionManager txManager, PageManager pageManager) throws Exception {
        super(path);
        this.txManager = txManager;
        this.pageManager = pageManager;
        this.checkpointManager = new CheckpointManager(this, txManager);
    }

    /**
     * 创建检查点 - 增强版实现
     * 这个方法扩展了基本实现，添加了更完整的检查点信息
     */
    @Override
    public void checkpoint() {
        LOGGER.info("开始创建增强版检查点...");

        try {
            // 锁定来确保一致性
            lock.lock();

            try {
                // 1. 首先刷新所有日志缓冲区内容到磁盘
                super.flush();

                // 2. 由于这个演示版本中没有直接访问脏页表和活跃事务表的方法
                // 我们可以通过以下方式构建这些信息

                // 获取活跃事务信息
                Map<Long, CheckpointManager.TransactionInfo> activeTransactions = getActiveTransactions();

                // 获取脏页信息
                Map<PageID, Long> dirtyPages = getDirtyPages();

                // 3. 使用检查点管理器创建包含完整信息的检查点
                checkpointManager.createCheckpoint(activeTransactions, dirtyPages);

                // 4. 刷新所有页面到磁盘
                pageManager.flushAll();

                // 5. 调用基本的checkpoint方法更新检查点位置
                super.checkpoint();

                LOGGER.info("增强版检查点创建完成");
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            LOGGER.severe("创建检查点失败: " + e.getMessage());
        }
    }

    /**
     * 获取当前活跃事务
     * 在实际系统中，这应该从事务管理器获取
     */
    private Map<Long, CheckpointManager.TransactionInfo> getActiveTransactions() {
        // 演示用，实际实现应该从事务管理器获取
        Map<Long, CheckpointManager.TransactionInfo> result = new ConcurrentHashMap<>();

        // 这里应该遍历所有活跃事务，但由于没有直接访问的方法，我们创建一个模拟实现
        // 在实际系统中，可能需要添加TransactionManager的API来获取这些信息

        // 假设事务ID从1到100，检查哪些是活跃的
        for (long xid = 1; xid <= 100; xid++) {
            if (txManager.isActive(xid)) {
                CheckpointManager.TransactionInfo txInfo = new CheckpointManager.TransactionInfo();
                txInfo.lastLSN = 0; // 在实际实现中，需要获取最后一条日志的LSN
                result.put(xid, txInfo);
            }
        }

        return result;
    }

    /**
     * 获取当前脏页信息
     * 在实际系统中，这应该从缓冲池管理器获取
     */
    private Map<PageID, Long> getDirtyPages() {
        // 演示用，实际实现应该从缓冲池管理器获取
        Map<PageID, Long> result = new ConcurrentHashMap<>();

        // 这里应该遍历所有脏页，但由于没有直接访问的方法，我们创建一个模拟实现
        // 在实际系统中，需要添加BufferPoolManager的API来获取这些信息

        return result;
    }

    /**
     * 用于支持CheckpointManager的PageID类
     */
    public static class PageID {
        private final int fileID;
        private final int pageNum;

        public PageID(int fileID, int pageNum) {
            this.fileID = fileID;
            this.pageNum = pageNum;
        }

        public int getFileID() {
            return fileID;
        }

        public int getPageNum() {
            return pageNum;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            PageID other = (PageID) obj;
            return fileID == other.fileID && pageNum == other.pageNum;
        }

        @Override
        public int hashCode() {
            return 31 * fileID + pageNum;
        }
    }
}
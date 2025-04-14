package org.lupenghan.eazydb.backend.DataManager;

import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.BufferPoolManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.DiskManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Impl.BufferPoolManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Impl.DiskManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Impl.PageManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManagerImpl;

import java.util.logging.Logger;

/**
 * 增强版DBSystem类，集成了ARIES恢复系统
 */
public class EnhancedDBSystem {
    private static final Logger LOGGER = Logger.getLogger(EnhancedDBSystem.class.getName());

    // 缓冲池大小
    private static final int DEFAULT_BUFFER_POOL_SIZE = 1024;

    private final TransactionManager tm;
    private final PageManager pageManager;
    private final ARIESRecoverySystem recoverySystem;
    private final String path;

    /**
     * 创建增强版数据库系统
     * @param path 数据库路径
     * @throws Exception 如果创建失败
     */
    public EnhancedDBSystem(String path) throws Exception {
        this.path = path;

        // 创建事务管理器
        this.tm = new TransactionManagerImpl(path);

        // 创建磁盘管理器和缓冲池管理器
        DiskManager diskManager = new DiskManagerImpl(path);
        BufferPoolManager bufferPoolManager = new BufferPoolManagerImpl(DEFAULT_BUFFER_POOL_SIZE, diskManager);

        // 创建页面管理器（此时没有日志管理器）
        this.pageManager = new PageManagerImpl(bufferPoolManager, null);

        // 创建ARIES恢复系统（包含日志管理器）
        this.recoverySystem = new ARIESRecoverySystem(path, pageManager, tm);

        // 将日志管理器关联到页面管理器（反向依赖注入）
        if (pageManager instanceof PageManagerImpl) {
            ((PageManagerImpl) pageManager).setLogManager(recoverySystem.getLogManager());
        }

        // 系统启动时执行恢复
        LOGGER.info("数据库系统启动，开始恢复过程...");
        recover();
        LOGGER.info("恢复过程完成，数据库系统就绪");
    }

    /**
     * 执行数据库恢复
     */
    private void recover() {
        recoverySystem.recover();
    }

    /**
     * 开始一个新事务
     * @return 事务ID
     */
    public long begin() {
        return tm.begin();
    }

    /**
     * 提交事务
     * @param xid 事务ID
     */
    public void commit(long xid) {
        tm.commit(xid);
    }

    /**
     * 中止事务
     * @param xid 事务ID
     */
    public void abort(long xid) {
        tm.abort(xid);
    }

    /**
     * 创建检查点
     */
    public void checkpoint() {
        recoverySystem.createCheckpoint();
    }

    /**
     * 关闭数据库系统
     */
    public void close() {
        LOGGER.info("关闭数据库系统...");

        // 先创建检查点
        checkpoint();

        // 关闭恢复系统（包括日志管理器）
        recoverySystem.shutdown();

        // 关闭页面管理器
        pageManager.close();

        // 关闭事务管理器
        tm.close();

        LOGGER.info("数据库系统已关闭");
    }

    /**
     * 获取事务管理器
     * @return 事务管理器
     */
    public TransactionManager getTransactionManager() {
        return tm;
    }

    /**
     * 获取页面管理器
     * @return 页面管理器
     */
    public PageManager getPageManager() {
        return pageManager;
    }

    /**
     * 获取日志管理器
     * @return 日志管理器
     */
    public LogManager getLogManager() {
        return recoverySystem.getLogManager();
    }

    /**
     * 获取恢复系统
     * @return 恢复系统
     */
    public ARIESRecoverySystem getRecoverySystem() {
        return recoverySystem;
    }
}
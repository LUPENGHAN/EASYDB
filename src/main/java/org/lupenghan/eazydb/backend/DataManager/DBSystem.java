package org.lupenghan.eazydb.backend.DataManager;

import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManagerImpl;
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
 * 数据库系统类，整合事务管理器、日志管理器和页面管理器
 * 集成了ARIES恢复功能
 */
public class DBSystem {
    private static final Logger LOGGER = Logger.getLogger(DBSystem.class.getName());

    // 缓冲池默认大小
    private static final int DEFAULT_BUFFER_POOL_SIZE = 1024;

    private final TransactionManager tm;
    private final LogManager lm;
    private final PageManager pageManager;
    private final ARIESRecoverySystem recoverySystem;
    private final String path;

    /**
     * 创建数据库系统实例
     * @param path 数据库路径
     * @throws Exception 如果创建失败
     */
    public DBSystem(String path) throws Exception {
        this.path = path;

        // 创建事务管理器
        this.tm = new TransactionManagerImpl(path);

        // 创建磁盘管理器和缓冲池管理器
        DiskManager diskManager = new DiskManagerImpl(path);
        BufferPoolManager bufferPoolManager = new BufferPoolManagerImpl(DEFAULT_BUFFER_POOL_SIZE, diskManager);

        // 创建页面管理器（初始时没有日志管理器）
        PageManagerImpl pageManagerImpl = new PageManagerImpl(bufferPoolManager, null);
        this.pageManager = pageManagerImpl;

        // 创建ARIES恢复系统（包含日志管理器）
        this.recoverySystem = new ARIESRecoverySystem(path, pageManager, tm);

        // 获取日志管理器
        this.lm = recoverySystem.getLogManager();

        // 将日志管理器关联到页面管理器（解决循环依赖）
        pageManagerImpl.setLogManager(lm);

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
        // 1. 记录提交日志
        // 2. 执行实际的提交
        tm.commit(xid);
    }

    /**
     * 中止事务
     * @param xid 事务ID
     */
    public void abort(long xid) {
        // 1. 记录中止日志
        // 2. 执行实际的中止
        tm.abort(xid);
    }

    /**
     * 创建检查点
     */
    public void checkpoint() {
        // 创建检查点
        lm.checkpoint();
    }

    /**
     * 关闭数据库系统
     */
    public void close() {
        LOGGER.info("关闭数据库系统...");

        // 先创建检查点
        lm.checkpoint();

        // 关闭各组件
        lm.close();
        pageManager.close();
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
     * 获取日志管理器
     * @return 日志管理器
     */
    public LogManager getLogManager() {
        return lm;
    }

    /**
     * 获取页面管理器
     * @return 页面管理器
     */
    public PageManager getPageManager() {
        return pageManager;
    }

    /**
     * 获取恢复系统
     * @return 恢复系统
     */
    public ARIESRecoverySystem getRecoverySystem() {
        return recoverySystem;
    }
}
package org.lupenghan.eazydb.backend.DataManager;

import lombok.Getter;
import org.lupenghan.eazydb.backend.DataManager.LogManager.CheckpointManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.EnhancedLogManagerImpl;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.RecoveryManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;

import java.util.logging.Logger;

/**
 * ARIES恢复系统整合类
 * 该类将各个组件整合在一起，提供完整的恢复功能
 */
public class ARIESRecoverySystem {
    private static final Logger LOGGER = Logger.getLogger(ARIESRecoverySystem.class.getName());


    @Getter
    private final RecoveryManager recoveryManager;

    @Getter
    private final LogManager logManager;
    private final PageManager pageManager;
    private final TransactionManager txManager;

    /**
     * 创建ARIES恢复系统
     * @param dbPath 数据库路径
     * @param pageManager 页面管理器
     * @param txManager 事务管理器
     * @throws Exception 如果创建失败
     */
    public ARIESRecoverySystem(String dbPath, PageManager pageManager, TransactionManager txManager) throws Exception {
        this.pageManager = pageManager;
        this.txManager = txManager;

        // 创建增强版日志管理器
        this.logManager = new EnhancedLogManagerImpl(dbPath, txManager, pageManager);

        // 创建恢复管理器
        this.recoveryManager = new RecoveryManager(logManager, pageManager, txManager);

        LOGGER.info("ARIES恢复系统初始化完成");
    }

    /**
     * 执行恢复过程
     */
    public void recover() {
        LOGGER.info("开始ARIES恢复过程");
        recoveryManager.recover();
        LOGGER.info("ARIES恢复过程完成");
    }

    /**
     * 创建检查点
     */
    public void createCheckpoint() {
        LOGGER.info("开始创建ARIES检查点");
        logManager.checkpoint();
        LOGGER.info("ARIES检查点创建完成");
    }

    /**
     * 关闭恢复系统
     */
    public void shutdown() {
        LOGGER.info("关闭ARIES恢复系统");

        try {
            // 创建一个最终检查点
            createCheckpoint();

            // 关闭日志管理器
            logManager.close();
        } catch (Exception e) {
            LOGGER.severe("关闭ARIES恢复系统失败: " + e.getMessage());
        }
    }

    /**
     * 在系统启动时执行恢复
     * 这个方法应该在DBSystem中调用
     */
    public static void performRecovery(String dbPath, PageManager pageManager, TransactionManager txManager) {
        try {
            ARIESRecoverySystem recoverySystem = new ARIESRecoverySystem(dbPath, pageManager, txManager);
            recoverySystem.recover();

            // 注意：在实际使用中，我们应该返回创建的恢复系统实例
            // 以便后续使用其中的日志管理器等组件
        } catch (Exception e) {
            LOGGER.severe("执行恢复过程失败: " + e.getMessage());
            throw new RuntimeException("恢复失败，系统无法启动", e);
        }
    }
}
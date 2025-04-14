package org.lupenghan.eazydb.backend.DataManager.PageManager.Impl;

import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.BufferPoolManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;

import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * 页面管理器的增强实现，支持动态设置日志管理器
 */
public class PageManagerImpl implements PageManager {
    private static final Logger LOGGER = Logger.getLogger(PageManagerImpl.class.getName());

    private final BufferPoolManager bufferPoolManager;
    private LogManager logManager;
    private final ReentrantLock lock;

    /**
     * 创建页面管理器
     * @param bufferPoolManager 缓冲池管理器
     * @param logManager 日志管理器，可以为null（后续通过setLogManager设置）
     */
    public PageManagerImpl(BufferPoolManager bufferPoolManager, LogManager logManager) {
        this.bufferPoolManager = bufferPoolManager;
        this.logManager = logManager;
        this.lock = new ReentrantLock();
    }

    /**
     * 设置日志管理器
     * 这个方法用于解决循环依赖问题
     * @param logManager 日志管理器
     */
    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
        LOGGER.info("日志管理器已设置到页面管理器");
    }

    @Override
    public Page pinPage(PageID pageID) {
        // 直接使用缓冲池管理器的pinPage方法
        return bufferPoolManager.pinPage(pageID);
    }

    @Override
    public void unpinPage(PageID pageID, boolean isDirty) {
        // 直接使用缓冲池管理器的unpinPage方法
        bufferPoolManager.unpinPage(pageID, isDirty);
    }

    @Override
    public byte[] read(PageID pageID, int offset, int length) {
        Page page = bufferPoolManager.pinPage(pageID);
        if (page == null) {
            throw new RuntimeException("无法访问页面: " + pageID);
        }

        try {
            return page.readData(offset, length);
        } finally {
            bufferPoolManager.unpinPage(pageID, false); // 读操作不会修改页面
        }
    }

    @Override
    public boolean write(PageID pageID, int offset, byte[] data, long xid) {
        lock.lock();
        try {
            if (logManager == null) {
                LOGGER.warning("日志管理器未设置，写入操作未记录日志");
                // 执行写入，但不记录日志
                Page page = bufferPoolManager.pinPage(pageID);
                if (page == null) {
                    return false;
                }

                try {
                    page.writeData(offset, data);
                    return true;
                } finally {
                    bufferPoolManager.unpinPage(pageID, true); // 写操作会修改页面
                }
            }

            Page page = bufferPoolManager.pinPage(pageID);
            if (page == null) {
                return false;
            }

            try {
                // 获取原始数据用于日志记录
                byte[] oldData = page.readData(offset, data.length);

                // 记录重做日志
                long lsn = logManager.appendRedoLog(xid, pageID.getPageNum(), (short)offset, oldData, data);

                // 更新页面数据
                page.writeData(offset, data);

                // 更新页面LSN
                page.setLSN(lsn);

                return true;
            } finally {
                bufferPoolManager.unpinPage(pageID, true); // 写操作会修改页面
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PageID allocatePage() {
        lock.lock();
        try {
            Page page = bufferPoolManager.newPage();
            if (page == null) {
                return null;
            }

            PageID pageID = page.getPageID();
            bufferPoolManager.unpinPage(pageID, false); // 新页面不是脏页

            return pageID;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean freePage(PageID pageID) {
        lock.lock();
        try {
            return bufferPoolManager.deletePage(pageID);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flushAll() {
        bufferPoolManager.flushAllPages();
    }

    @Override
    public void flushPage(PageID pageID) {
        bufferPoolManager.flushPage(pageID);
    }

    @Override
    public void checkpoint() {
        lock.lock();
        try {
            // 首先刷新所有脏页
            bufferPoolManager.flushAllPages();

            // 然后创建日志检查点
            if (logManager != null) {
                logManager.checkpoint();
            } else {
                LOGGER.warning("日志管理器未设置，无法创建检查点");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void recover() {
        lock.lock();
        try {
            // 如果日志管理器不存在，无法执行恢复
            if (logManager == null) {
                LOGGER.warning("日志管理器未设置，无法执行恢复");
                return;
            }

            // 执行日志恢复过程
            logManager.recover();

            // 恢复后刷新所有页面，确保数据持久化
            bufferPoolManager.flushAllPages();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            // 创建检查点
            checkpoint();

            // 确保所有脏页都写入磁盘
            bufferPoolManager.flushAllPages();
        } finally {
            lock.unlock();
        }
    }
}
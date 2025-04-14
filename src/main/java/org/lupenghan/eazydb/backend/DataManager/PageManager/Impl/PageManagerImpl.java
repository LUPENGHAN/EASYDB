package org.lupenghan.eazydb.backend.DataManager.PageManager.Impl;

import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.BufferPoolManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面管理器的实现
 */
public class PageManagerImpl implements PageManager {
    private final BufferPoolManager bufferPoolManager;
    private final LogManager logManager;
    private final ReentrantLock lock;

    /**
     * 创建页面管理器
     * @param bufferPoolManager 缓冲池管理器
     * @param logManager 日志管理器
     */
    public PageManagerImpl(BufferPoolManager bufferPoolManager, LogManager logManager) {
        this.bufferPoolManager = bufferPoolManager;
        this.logManager = logManager;
        this.lock = new ReentrantLock();
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
            logManager.checkpoint();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void recover() {
        lock.lock();
        try {
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
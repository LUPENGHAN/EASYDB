package org.lupenghan.eazydb.backend.DataManager.PageManager.Impl;

import org.lupenghan.eazydb.backend.DataManager.PageManager.BufferPoolManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.DiskManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 缓冲池管理器的实现
 */
public class BufferPoolManagerImpl implements BufferPoolManager {
    private final int poolSize;                        // 缓冲池大小
    private final Page[] pages;                        // 页面数组
    private final Map<PageID, Integer> pageTable;      // 页面ID到页面数组索引的映射
    private final Queue<Integer> freeList;             // 空闲页框列表
    private final ReentrantLock lock;                  // 并发控制锁
    private final DiskManager diskManager;             // 磁盘管理器

    /**
     * 创建一个指定大小的缓冲池管理器
     * @param poolSize 缓冲池的大小（页面数量）
     * @param diskManager 磁盘管理器
     */
    public BufferPoolManagerImpl(int poolSize, DiskManager diskManager) {
        this.poolSize = poolSize;
        this.pages = new Page[poolSize];
        this.pageTable = new HashMap<>();
        this.freeList = new LinkedList<>();
        this.lock = new ReentrantLock();
        this.diskManager = diskManager;

        // 初始化空闲页框列表
        for (int i = 0; i < poolSize; i++) {
            freeList.offer(i);
        }
    }

    @Override
    public Page fetchPage(PageID pageID) {
        lock.lock();
        try {
            // 如果页面已经在缓冲池中，直接返回
            if (pageTable.containsKey(pageID)) {
                int frameId = pageTable.get(pageID);
                pages[frameId].incrementPinCount();
                return pages[frameId];
            }

            // 找一个空闲页框
            int frameId = findFreeFrame();
            if (frameId == -1) {
                // 缓冲池已满，且所有页面都被固定，无法替换
                return null;
            }

            // 如果页框中已有页面，且是脏页，先将其写回磁盘
            if (pages[frameId] != null) {
                Page oldPage = pages[frameId];
                if (oldPage.isDirty()) {
                    diskManager.writePage(oldPage.getPageID(), oldPage.getData());
                }
                // 从页表中移除旧页面
                pageTable.remove(oldPage.getPageID());
            }

            // 从磁盘读取页面
            byte[] pageData = diskManager.readPage(pageID);
            Page page;
            if (pageData != null) {
                page = new PageImpl(pageID, pageData);
            } else {
                // 页面不存在，创建新页面
                page = new PageImpl(pageID);
            }

            // 将页面放入缓冲池
            pages[frameId] = page;
            pageTable.put(pageID, frameId);
            page.incrementPinCount();

            return page;
        } catch (IOException e) {
            // 处理IO异常
            e.printStackTrace();
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page newPage() {
        lock.lock();
        try {
            // 找一个空闲页框
            int frameId = findFreeFrame();
            if (frameId == -1) {
                // 缓冲池已满，且所有页面都被固定，无法替换
                return null;
            }

            // 如果页框中已有页面，且是脏页，先将其写回磁盘
            if (pages[frameId] != null) {
                Page oldPage = pages[frameId];
                if (oldPage.isDirty()) {
                    diskManager.writePage(oldPage.getPageID(), oldPage.getData());
                }
                // 从页表中移除旧页面
                pageTable.remove(oldPage.getPageID());
            }

            // 从磁盘管理器获取新页面ID
            PageID pageID = diskManager.allocatePage();
            if (pageID == null) {
                return null;
            }

            // 创建新页面
            Page page = new PageImpl(pageID);

            // 将页面放入缓冲池
            pages[frameId] = page;
            pageTable.put(pageID, frameId);
            page.incrementPinCount();

            return page;
        } catch (IOException e) {
            // 处理IO异常
            e.printStackTrace();
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deletePage(PageID pageID) {
        lock.lock();
        try {
            // 如果页面在缓冲池中
            if (pageTable.containsKey(pageID)) {
                int frameId = pageTable.get(pageID);
                Page page = pages[frameId];

                // 如果页面仍被固定，无法删除
                if (page.getPinCount() > 0) {
                    return false;
                }

                // 从缓冲池中移除页面
                pages[frameId] = null;
                pageTable.remove(pageID);
                freeList.offer(frameId);
            }

            // 从磁盘中删除页面
            return diskManager.deallocatePage(pageID);
        } catch (IOException e) {
            // 处理IO异常
            e.printStackTrace();
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page pinPage(PageID pageID) {
        return fetchPage(pageID); // fetchPage已经增加了pin计数
    }

    @Override
    public boolean unpinPage(PageID pageID, boolean isDirty) {
        lock.lock();
        try {
            if (!pageTable.containsKey(pageID)) {
                return false; // 页面不在缓冲池中
            }

            int frameId = pageTable.get(pageID);
            Page page = pages[frameId];

            // 更新脏页标志
            if (isDirty) {
                page.setDirty(true);
            }

            // 减少pin计数
            page.decrementPinCount();

            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean flushAllPages() {
        lock.lock();
        try {
            boolean success = true;

            for (int i = 0; i < poolSize; i++) {
                if (pages[i] != null && pages[i].isDirty()) {
                    try {
                        diskManager.writePage(pages[i].getPageID(), pages[i].getData());
                        pages[i].setDirty(false);
                    } catch (IOException e) {
                        // 处理IO异常
                        e.printStackTrace();
                        success = false;
                    }
                }
            }

            return success;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean flushPage(PageID pageID) {
        lock.lock();
        try {
            if (!pageTable.containsKey(pageID)) {
                return false; // 页面不在缓冲池中
            }

            int frameId = pageTable.get(pageID);
            Page page = pages[frameId];

            if (page.isDirty()) {
                try {
                    diskManager.writePage(page.getPageID(), page.getData());
                    page.setDirty(false);
                    return true;
                } catch (IOException e) {
                    // 处理IO异常
                    e.printStackTrace();
                    return false;
                }
            }

            return true; // 页面不是脏页，无需刷新
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getPoolSize() {
        return poolSize;
    }

    @Override
    public int getAvailableFrames() {
        lock.lock();
        try {
            return freeList.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查找一个可用的页框
     * @return 可用页框的索引，如果没有可用页框则返回-1
     */
    private int findFreeFrame() {
        // 如果有空闲页框，直接使用
        if (!freeList.isEmpty()) {
            return freeList.poll();
        }

        // 尝试找一个未被固定的页面进行替换
        for (int i = 0; i < poolSize; i++) {
            if (pages[i] != null && pages[i].getPinCount() == 0) {
                return i;
            }
        }

        // 所有页面都被固定，无法替换
        return -1;
    }
}
package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Impl;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.SlottedPageImpl;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordPage;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.TablespaceManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 表空间管理器实现类
 */
public class TablespaceManagerImpl implements TablespaceManager {
    // 表空间元数据文件前缀
    private static final String TABLE_META_PREFIX = "table_meta_";

    // 页面信息记录大小
    private static final int PAGE_INFO_SIZE = 12; // pageID(8) + freeSpace(4)

    // 页面管理器
    private final PageManager pageManager;

    // 数据库目录
    private final String dbDirectory;

    // 表的页面映射：tableID -> List<PageID>
    private final Map<Integer, List<PageID>> tablePages;

    // 页面空闲空间映射：pageID -> Integer
    private final Map<PageID, Integer> pageFreeSpace;

    // 锁
    private final ReentrantReadWriteLock lock;

    /**
     * 创建表空间管理器
     * @param pageManager 页面管理器
     * @param dbDirectory 数据库目录
     */
    public TablespaceManagerImpl(PageManager pageManager, String dbDirectory) {
        this.pageManager = pageManager;
        this.dbDirectory = dbDirectory;
        this.tablePages = new ConcurrentHashMap<>();
        this.pageFreeSpace = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();

        // 确保目录存在
        File dir = new File(dbDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // 加载表元数据
        loadTableMetadata();
    }

    /**
     * 加载所有表的元数据
     */
    private void loadTableMetadata() {
        File dir = new File(dbDirectory);
        File[] files = dir.listFiles((d, name) -> name.startsWith(TABLE_META_PREFIX));

        if (files != null) {
            for (File file : files) {
                // 从文件名提取表ID
                String fileName = file.getName();
                String tableIdStr = fileName.substring(TABLE_META_PREFIX.length());
                int tableId = Integer.parseInt(tableIdStr);

                // 加载表的页面信息
                loadTablePages(tableId);
            }
        }
    }

    /**
     * 加载指定表的页面信息
     * @param tableID 表ID
     */
    private void loadTablePages(int tableID) {
        String metaFilePath = dbDirectory + File.separator + TABLE_META_PREFIX + tableID;
        File metaFile = new File(metaFilePath);

        if (!metaFile.exists()) {
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(metaFile, "r");
             FileChannel channel = raf.getChannel()) {

            // 获取文件大小
            long fileSize = channel.size();
            int pageCount = (int) (fileSize / PAGE_INFO_SIZE);

            // 读取所有页面信息
            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
            channel.read(buffer);
            buffer.flip();

            // 为该表创建页面列表
            List<PageID> pages = new ArrayList<>();
            tablePages.put(tableID, pages);

            // 解析页面信息
            for (int i = 0; i < pageCount; i++) {
                int fileID = buffer.getInt();
                int pageNum = buffer.getInt();
                int freeSpace = buffer.getInt();

                PageID pageID = new PageID(fileID, pageNum);
                pages.add(pageID);
                pageFreeSpace.put(pageID, freeSpace);
            }
        } catch (IOException e) {
            throw new RuntimeException("加载表页面信息失败: tableID=" + tableID, e);
        }
    }

    /**
     * 保存表的页面信息
     * @param tableID 表ID
     */
    private void saveTablePages(int tableID) {
        lock.writeLock().lock();
        try {
            List<PageID> pages = tablePages.get(tableID);
            if (pages == null || pages.isEmpty()) {
                return;
            }

            String metaFilePath = dbDirectory + File.separator + TABLE_META_PREFIX + tableID;

            try (RandomAccessFile raf = new RandomAccessFile(metaFilePath, "rw");
                 FileChannel channel = raf.getChannel()) {

                // 分配缓冲区
                int size = pages.size() * PAGE_INFO_SIZE;
                ByteBuffer buffer = ByteBuffer.allocate(size);

                // 写入所有页面信息
                for (PageID pageID : pages) {
                    buffer.putInt(pageID.getFileID());
                    buffer.putInt(pageID.getPageNum());
                    buffer.putInt(pageFreeSpace.getOrDefault(pageID, 0));
                }

                buffer.flip();
                channel.truncate(0); // 清空文件
                channel.write(buffer);
                channel.force(true); // 确保写入磁盘
            } catch (IOException e) {
                throw new RuntimeException("保存表页面信息失败: tableID=" + tableID, e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public PageID allocatePage(int tableID) throws Exception {
        lock.writeLock().lock();
        try {
            // 获取该表的页面列表，如果不存在则创建
            List<PageID> pages = tablePages.computeIfAbsent(tableID, k -> new ArrayList<>());

            // 分配新页面
            PageID pageID = pageManager.allocatePage();
            if (pageID == null) {
                throw new Exception("无法分配新页面");
            }

            // 初始化新页面
            Page page = pageManager.pinPage(pageID);
            if (page == null) {
                throw new Exception("无法访问新页面: " + pageID);
            }

            try {
                // 创建槽式页面并获取可用空间
                RecordPage recordPage = new SlottedPageImpl(page);
                int freeSpace = recordPage.getFreeSpace();

                // 更新缓存
                pages.add(pageID);
                pageFreeSpace.put(pageID, freeSpace);

                // 保存元数据
                saveTablePages(tableID);

                return pageID;
            } finally {
                pageManager.unpinPage(pageID, true); // 标记为脏页，确保写入
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void freePage(PageID pageID, int tableID) throws Exception {
        lock.writeLock().lock();
        try {
            // 获取该表的页面列表
            List<PageID> pages = tablePages.get(tableID);
            if (pages == null) {
                return;
            }

            // 从列表中移除
            if (pages.remove(pageID)) {
                // 从空闲空间映射中移除
                pageFreeSpace.remove(pageID);

                // 释放页面
                pageManager.freePage(pageID);

                // 保存元数据
                saveTablePages(tableID);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<PageID> getTablePages(int tableID) throws Exception {
        lock.readLock().lock();
        try {
            // 返回该表的页面列表的副本
            List<PageID> pages = tablePages.get(tableID);
            if (pages == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(pages);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public PageID getPageWithSpace(int tableID, int requiredSpace) throws Exception {
        lock.readLock().lock();
        try {
            // 获取该表的页面列表
            List<PageID> pages = tablePages.get(tableID);
            if (pages == null || pages.isEmpty()) {
                return null;
            }

            // 更新空闲空间信息
            updatePageFreeSpace(tableID);

            // 寻找有足够空间的页面
            for (PageID pageID : pages) {
                Integer freeSpace = pageFreeSpace.get(pageID);
                if (freeSpace != null && freeSpace >= requiredSpace) {
                    return pageID;
                }
            }

            return null; // 没有找到合适的页面
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 更新表中所有页面的空闲空间信息
     * @param tableID 表ID
     */
    private void updatePageFreeSpace(int tableID) {
        // 获取该表的页面列表
        List<PageID> pages = tablePages.get(tableID);
        if (pages == null || pages.isEmpty()) {
            return;
        }

        // 更新每个页面的空闲空间信息
        for (PageID pageID : pages) {
            Page page = pageManager.pinPage(pageID);
            if (page == null) {
                // 如果页面无法访问，可能已被删除，从缓存中移除
                pageFreeSpace.remove(pageID);
                continue;
            }

            try {
                // 创建槽式页面并获取可用空间
                RecordPage recordPage = new SlottedPageImpl(page);
                int freeSpace = recordPage.getFreeSpace();

                // 更新缓存
                pageFreeSpace.put(pageID, freeSpace);
            } finally {
                pageManager.unpinPage(pageID, false); // 读操作不修改页面
            }
        }
    }

    /**
     * 关闭表空间管理器，保存所有元数据
     */
    public void close() {
        lock.writeLock().lock();
        try {
            // 保存所有表的页面信息
            for (int tableID : tablePages.keySet()) {
                saveTablePages(tableID);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Impl;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordHeader;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.Record;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.SlottedPageImpl;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordManager;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordPage;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.TablespaceManager;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 记录管理器实现类
 */
public class RecordManagerImpl implements RecordManager {
    private final PageManager pageManager;
    private final TransactionManager txManager;
    private final LogManager logManager;
    private final TablespaceManager tablespaceManager;
    private final int tableID;  // 当前表ID
    private final ReentrantLock lock;

    /**
     * 创建记录管理器
     * @param pageManager 页面管理器
     * @param txManager 事务管理器
     * @param logManager 日志管理器
     * @param tablespaceManager 表空间管理器
     * @param tableID 表ID
     */
    public RecordManagerImpl(PageManager pageManager, TransactionManager txManager,
                             LogManager logManager, TablespaceManager tablespaceManager,
                             int tableID) {
        this.pageManager = pageManager;
        this.txManager = txManager;
        this.logManager = logManager;
        this.tablespaceManager = tablespaceManager;
        this.tableID = tableID;
        this.lock = new ReentrantLock();
    }

    @Override
    public RecordID insertRecord(byte[] data, long xid) throws Exception {
        if (!txManager.isActive(xid)) {
            throw new Exception("事务 " + xid + " 不是活动的");
        }

        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("记录数据不能为空");
        }

        // 构建完整记录（包括头部）
        byte[] fullRecord = buildRecordWithHeader(data, RecordHeader.VALID, xid);

        lock.lock();
        try {
            // 寻找有足够空间的页面
            PageID pageID = tablespaceManager.getPageWithSpace(tableID, fullRecord.length);

            if (pageID == null) {
                // 分配新页面
                pageID = tablespaceManager.allocatePage(tableID);
                if (pageID == null) {
                    throw new Exception("无法分配新页面");
                }
            }

            // 获取页面
            Page page = pageManager.pinPage(pageID);
            if (page == null) {
                throw new Exception("无法访问页面：" + pageID);
            }

            try {
                // 创建槽式页面
                RecordPage recordPage = new SlottedPageImpl(page);

                // 插入记录
                int slotNum = recordPage.insertRecord(fullRecord);

                // 返回记录ID
                return new RecordID(pageID, slotNum);
            } finally {
                pageManager.unpinPage(pageID, true); // 标记为脏页
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deleteRecord(RecordID rid, long xid) throws Exception {
        if (!txManager.isActive(xid)) {
            throw new Exception("事务 " + xid + " 不是活动的");
        }

        lock.lock();
        try {
            // 获取记录所在页面
            PageID pageID = rid.getPageID();
            Page page = pageManager.pinPage(pageID);

            if (page == null) {
                return false;
            }

            try {
                // 创建槽式页面
                RecordPage recordPage = new SlottedPageImpl(page);

                // 读取当前记录
                byte[] currentRecord = recordPage.getRecord(rid.getSlotNum());
                if (currentRecord == null) {
                    return false;
                }

                // 提取记录头部
                RecordHeader header = RecordHeader.deserialize(currentRecord);

                // 检查记录是否已被删除
                if (header.getStatus() == RecordHeader.DELETED) {
                    return false;
                }

                // 更新记录头部为删除状态
                header.setStatus(RecordHeader.DELETED);
                header.setXid(xid);

                // 构建新记录（只更新头部）
                byte[] newHeaderBytes = header.serialize();
                System.arraycopy(newHeaderBytes, 0, currentRecord, 0, RecordHeader.HEADER_SIZE);

                // 更新记录
                recordPage.updateRecord(rid.getSlotNum(), currentRecord);

                return true;
            } finally {
                pageManager.unpinPage(pageID, true); // 标记为脏页
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean updateRecord(RecordID rid, byte[] newData, long xid) throws Exception {
        if (!txManager.isActive(xid)) {
            throw new Exception("事务 " + xid + " 不是活动的");
        }

        if (newData == null || newData.length == 0) {
            throw new IllegalArgumentException("记录数据不能为空");
        }

        lock.lock();
        try {
            // 获取记录所在页面
            PageID pageID = rid.getPageID();
            Page page = pageManager.pinPage(pageID);

            if (page == null) {
                return false;
            }

            try {
                // 创建槽式页面
                RecordPage recordPage = new SlottedPageImpl(page);

                // 读取当前记录
                byte[] currentRecord = recordPage.getRecord(rid.getSlotNum());
                if (currentRecord == null) {
                    return false;
                }

                // 提取记录头部
                RecordHeader header = RecordHeader.deserialize(currentRecord);

                // 检查记录是否已被删除
                if (header.getStatus() == RecordHeader.DELETED) {
                    return false;
                }

                // 构建新记录（包括头部）
                byte[] fullRecord = buildRecordWithHeader(newData, RecordHeader.VALID, xid);

                // 如果新记录大小与旧记录不同，可能需要页面重排或分配新页面
                if (fullRecord.length != currentRecord.length) {
                    // 尝试在当前页面更新
                    try {
                        recordPage.updateRecord(rid.getSlotNum(), fullRecord);
                    } catch (Exception e) {
                        // 当前页面空间不足，删除旧记录
                        recordPage.deleteRecord(rid.getSlotNum());

                        // 释放页面
                        pageManager.unpinPage(pageID, true);

                        // 插入新记录
                        RecordID newRid = insertRecord(newData, xid);

                        // 如果新记录ID与旧记录ID不同，则返回false
                        // （这意味着记录已移动到新位置，调用者需要处理）
                        return newRid.equals(rid);
                    }
                } else {
                    // 记录大小相同，直接更新
                    recordPage.updateRecord(rid.getSlotNum(), fullRecord);
                }

                return true;
            } finally {
                pageManager.unpinPage(pageID, true); // 标记为脏页
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] getRecord(RecordID rid, long xid) throws Exception {
        lock.lock();
        try {
            // 获取记录所在页面
            PageID pageID = rid.getPageID();
            Page page = pageManager.pinPage(pageID);

            if (page == null) {
                return null;
            }

            try {
                // 创建槽式页面
                RecordPage recordPage = new SlottedPageImpl(page);

                // 读取记录
                byte[] fullRecord = recordPage.getRecord(rid.getSlotNum());
                if (fullRecord == null) {
                    return null;
                }

                // 提取记录头部
                RecordHeader header = RecordHeader.deserialize(fullRecord);

                // 检查记录是否已被删除
                if (header.getStatus() == RecordHeader.DELETED) {
                    return null;
                }

                // MVCC检查：检查事务可见性
                // (这里简化处理，实际实现需要考虑更复杂的MVCC规则)
                if (!isVisible(header.getXid(), xid)) {
                    return null;
                }

                // 提取数据部分（跳过头部）
                byte[] data = new byte[fullRecord.length - RecordHeader.HEADER_SIZE];
                System.arraycopy(fullRecord, RecordHeader.HEADER_SIZE, data, 0, data.length);

                return data;
            } finally {
                pageManager.unpinPage(pageID, false); // 读操作不修改页面
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterator<Record> scanRecords(Predicate<Record> predicate, long xid) throws Exception {
        lock.lock();
        try {
            // 获取表的所有页面
            List<PageID> tablePages = tablespaceManager.getTablePages(tableID);
            List<Record> records = new ArrayList<>();

            // 扫描所有页面
            for (PageID pageID : tablePages) {
                Page page = pageManager.pinPage(pageID);

                if (page == null) {
                    continue;
                }

                try {
                    // 创建槽式页面
                    RecordPage recordPage = new SlottedPageImpl(page);

                    // 获取所有有效槽
                    Iterator<Integer> slots = recordPage.getValidSlots();

                    while (slots.hasNext()) {
                        int slotNum = slots.next();

                        // 读取记录
                        byte[] fullRecord = recordPage.getRecord(slotNum);
                        if (fullRecord == null) {
                            continue;
                        }

                        // 提取记录头部
                        RecordHeader header = RecordHeader.deserialize(fullRecord);

                        // 检查记录是否已被删除
                        if (header.getStatus() == RecordHeader.DELETED) {
                            continue;
                        }

                        // MVCC检查：检查事务可见性
                        if (!isVisible(header.getXid(), xid)) {
                            continue;
                        }

                        // 提取数据部分
                        byte[] data = new byte[fullRecord.length - RecordHeader.HEADER_SIZE];
                        System.arraycopy(fullRecord, RecordHeader.HEADER_SIZE, data, 0, data.length);

                        // 创建记录对象
                        Record record = new Record(
                                new RecordID(pageID, slotNum),
                                data,
                                header.getStatus(),
                                0, // 这里简化处理，实际应该有版本时间戳
                                header.getXid()
                        );

                        // 应用谓词过滤
                        if (predicate == null || predicate.test(record)) {
                            records.add(record);
                        }
                    }
                } finally {
                    pageManager.unpinPage(pageID, false); // 读操作不修改页面
                }
            }

            return records.iterator();
        } finally {
            lock.unlock();
        }
    }
    /**
     * 构建带头部的完整记录
     * @param data 记录数据
     * @param status 记录状态
     * @param xid 事务ID
     * @return 完整记录字节数组
     */
    private byte[] buildRecordWithHeader(byte[] data, byte status, long xid) {
        // 计算记录总长度
        short length = (short) (RecordHeader.HEADER_SIZE + data.length);

        // 创建记录头部
        RecordHeader header = new RecordHeader(length, status, xid);
        byte[] headerBytes = header.serialize();

        // 构建完整记录
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(headerBytes);
        buffer.put(data);

        return buffer.array();
    }

    /**
     * 检查记录是否对指定事务可见
     * @param recordXid 记录的事务ID
     * @param currentXid 当前事务ID
     * @return 如果记录对当前事务可见则返回true
     */
    private boolean isVisible(long recordXid, long currentXid) {
        // 如果是同一个事务，总是可见
        if (recordXid == currentXid) {
            return true;
        }

        // 如果记录事务已提交，则可见
        // (这里简化处理，实际实现需要考虑更复杂的MVCC规则，如快照隔离)
        return txManager.isCommitted(recordXid);
    }

    /**
     * 获取页面
     * @param pageID 页面ID
     * @return 页面对象
     */
    private Page pinPage(PageID pageID) {
        try {
            return pageManager.pinPage(pageID);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 释放页面
     * @param pageID 页面ID
     * @param isDirty 是否为脏页
     */
    private void unpinPage(PageID pageID, boolean isDirty) {
        try {
            pageManager.unpinPage(pageID, isDirty);
        } catch (Exception e) {
            // 忽略异常
        }
    }
}
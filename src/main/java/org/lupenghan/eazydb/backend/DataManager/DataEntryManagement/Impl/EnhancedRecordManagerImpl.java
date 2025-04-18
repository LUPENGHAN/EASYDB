package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Impl;

import lombok.Getter;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.*;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.Record;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.MVCCRecordManager;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordPage;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.TablespaceManager;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.VersionStore;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.TransactionManager;
import org.lupenghan.eazydb.backend.TransactionManager.utils.ReadView;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * 支持MVCC的记录管理器实现，整合了基本记录操作和MVCC操作
 */
public class EnhancedRecordManagerImpl implements MVCCRecordManager {
    private final PageManager pageManager;
    private final TransactionManager txManager;
    @Getter
    private final LogManager logManager;
    private final TablespaceManager tablespaceManager;
    private final int tableID;
    private final ReentrantLock lock;

    // 版本存储
    private final VersionStore versionStore;

    // 标记是否启用MVCC
    private final boolean mvccEnabled;

    /**
     * 创建增强记录管理器，支持MVCC
     */
    public EnhancedRecordManagerImpl(PageManager pageManager, TransactionManager txManager,
                                     LogManager logManager, TablespaceManager tablespaceManager,
                                     VersionStore versionStore, int tableID, boolean mvccEnabled) {
        this.pageManager = pageManager;
        this.txManager = txManager;
        this.logManager = logManager;
        this.tablespaceManager = tablespaceManager;
        this.versionStore = versionStore;
        this.tableID = tableID;
        this.lock = new ReentrantLock();
        this.mvccEnabled = mvccEnabled;
    }

    /**
     * 创建基本记录管理器，不使用MVCC
     */
    public EnhancedRecordManagerImpl(PageManager pageManager, TransactionManager txManager,
                                     LogManager logManager, TablespaceManager tablespaceManager,
                                     int tableID) {
        this(pageManager, txManager, logManager, tablespaceManager, null, tableID, false);
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
                RecordID rid = new RecordID(pageID, slotNum);

                // 如果启用MVCC，创建初始版本
                if (mvccEnabled && versionStore != null) {
                    long beginTS = txManager.getBeginTimestamp(xid);
                    RecordVersion initialVersion = new RecordVersion(
                            rid,
                            xid,
                            beginTS,
                            RecordHeader.INFINITY_TS,
                            data,
                            RecordHeader.VALID,
                            RecordHeader.NULL_POINTER
                    );
                    versionStore.addVersion(initialVersion);
                }

                return rid;
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

        // 如果启用MVCC，创建删除标记版本
        if (mvccEnabled && versionStore != null) {
            long beginTS = txManager.getBeginTimestamp(xid);
            return deleteRecordVersion(rid, xid, beginTS);
        }

        // 否则使用传统方式删除
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

        // 如果启用MVCC，创建新版本
        if (mvccEnabled && versionStore != null) {
            long beginTS = txManager.getBeginTimestamp(xid);
            return updateRecordVersion(rid, newData, xid, beginTS);
        }

        // 否则使用传统方式更新
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
        // 如果启用MVCC，使用MVCC可见性规则读取记录
        if (mvccEnabled && versionStore != null) {
            // 为当前事务创建ReadView
            ReadView readView = txManager.createReadView(xid);
            return getRecordWithMVCC(rid, readView);
        }

        // 否则使用传统方式读取
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

                // 简单的可见性检查：只能看到已提交事务的记录
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
        // 如果启用MVCC，使用MVCC可见性规则扫描
        if (mvccEnabled && versionStore != null) {
            // 获取当前事务的ReadView
            ReadView readView = txManager.createReadView(xid);

            // 创建结果列表
            List<Record> visibleRecords = new ArrayList<>();

            // 获取表的所有页面
            List<PageID> tablePages = tablespaceManager.getTablePages(tableID);

            // 遍历所有页面
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
                        RecordID rid = new RecordID(pageID, slotNum);

                        // 获取所有版本
                        List<RecordVersion> versions = getAllVersions(rid);

                        if (!versions.isEmpty()) {
                            // 按MVCC可见性规则找到可见版本
                            for (RecordVersion version : versions) {
                                // 检查版本是否对当前事务可见
                                if (readView.isVisible(version.getXid(), version.getBeginTS(), version.getEndTS())) {
                                    // 创建可见记录
                                    Record visibleRecord = new Record(
                                            rid,
                                            version.getData(),
                                            version.getStatus(),
                                            version.getBeginTS(),
                                            version.getXid()
                                    );

                                    // 应用谓词过滤
                                    if (predicate == null || predicate.test(visibleRecord)) {
                                        visibleRecords.add(visibleRecord);
                                    }

                                    // 找到第一个可见版本后，不再处理该记录的其他版本
                                    break;
                                }
                            }
                        }
                    }
                } finally {
                    pageManager.unpinPage(pageID, false);
                }
            }

            return visibleRecords.iterator();
        }

        // 否则使用传统方式扫描
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

                        // 检查可见性
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

    // MVCC方法实现
    @Override
    public byte[] getRecordWithMVCC(RecordID rid, ReadView readView) throws Exception {
        // 获取记录的所有版本
        List<RecordVersion> versions = getAllVersions(rid);

        if (versions.isEmpty()) {
            return null;
        }

        // 按照MVCC可见性规则找到可见版本
        for (RecordVersion version : versions) {
            // 检查版本是否对当前事务可见
            if (readView.isVisible(version.getXid(), version.getBeginTS(), version.getEndTS())) {
                // 找到可见版本，返回数据
                return version.getData();
            }
        }

        // 没有找到可见版本
        return null;
    }

    @Override
    public List<RecordVersion> getAllVersions(RecordID rid) throws Exception {
        if (!mvccEnabled || versionStore == null) {
            // 如果未启用MVCC，返回空列表
            return new ArrayList<>();
        }
        return versionStore.getVersionChain(rid);
    }

    @Override
    public RecordVersion getRecordVersion(RecordID rid, long timestamp) throws Exception {
        if (!mvccEnabled || versionStore == null) {
            return null;
        }

        // 获取所有版本
        List<RecordVersion> versions = getAllVersions(rid);

        // 找到时间戳最接近的版本
        for (RecordVersion version : versions) {
            if (version.getBeginTS() <= timestamp && version.getEndTS() > timestamp) {
                return version;
            }
        }

        return null;
    }

    @Override
    public boolean updateRecordVersion(RecordID rid, byte[] data, long xid, long beginTS) throws Exception {
        if (!mvccEnabled || versionStore == null) {
            // 如果未启用MVCC，使用传统方式更新
            return updateRecord(rid, data, xid);
        }

        lock.lock();
        try {
            // 获取最新版本
            RecordVersion latestVersion = versionStore.getLatestVersion(rid);

            // 创建新版本
            RecordVersion newVersion = new RecordVersion(
                    rid,
                    xid,
                    beginTS,
                    RecordHeader.INFINITY_TS,
                    data,
                    RecordHeader.VALID,
                    latestVersion != null ? latestVersion.getRecordID().hashCode() : RecordHeader.NULL_POINTER
            );

            // 添加新版本
            boolean success = versionStore.addVersion(newVersion);

            // 如果有最新版本，更新其结束时间戳
            if (success && latestVersion != null) {
                versionStore.updateVersionEndTS(latestVersion.getRecordID().hashCode(), beginTS);
            }

            return success;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deleteRecordVersion(RecordID rid, long xid, long beginTS) throws Exception {
        if (!mvccEnabled || versionStore == null) {
            // 如果未启用MVCC，使用传统方式删除
            return deleteRecord(rid, xid);
        }

        lock.lock();
        try {
            // 获取最新版本
            RecordVersion latestVersion = versionStore.getLatestVersion(rid);

            if (latestVersion == null) {
                return false;
            }

            // 创建删除标记版本
            RecordVersion deleteVersion = new RecordVersion(
                    rid,
                    xid,
                    beginTS,
                    RecordHeader.INFINITY_TS,
                    new byte[0], // 空数据
                    RecordHeader.DELETED,
                    latestVersion.getRecordID().hashCode()
            );

            // 添加删除版本
            boolean success = versionStore.addVersion(deleteVersion);

            // 更新最新版本的结束时间戳
            if (success) {
                versionStore.updateVersionEndTS(latestVersion.getRecordID().hashCode(), beginTS);
            }

            return success;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int purgeOldVersions(long olderThan) throws Exception {
        if (!mvccEnabled || versionStore == null) {
            return 0;
        }
        return versionStore.purgeOldVersions(olderThan);
    }

// 辅助方法

    /**
     * 构建带头部的完整记录
     *
     * @param data   记录数据
     * @param status 记录状态
     * @param xid    事务ID
     * @return 完整记录字节数组
     */
    private byte[] buildRecordWithHeader(byte[] data, byte status, long xid) {
        // 计算记录总长度
        short length = (short) (RecordHeader.HEADER_SIZE + data.length);

        // 创建记录头部
        RecordHeader header = new RecordHeader(length, status, xid,
                txManager.getBeginTimestamp(xid), RecordHeader.INFINITY_TS, RecordHeader.NULL_POINTER);
        byte[] headerBytes = header.serialize();

        // 构建完整记录
        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.put(headerBytes);
        buffer.put(data);

        return buffer.array();
    }

    /**
     * 检查记录是否对指定事务可见
     *
     * @param recordXid  记录的事务ID
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

}
package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Impl;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.Record;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordHeader;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordVersion;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.MVCCRecordManager;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordManager;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.VersionStore;
import org.lupenghan.eazydb.backend.DataManager.LogManager.LogManager;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;
import org.lupenghan.eazydb.backend.TransactionManager.MVCCTransactionManager;
import org.lupenghan.eazydb.backend.TransactionManager.ReadView;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * 支持MVCC的记录管理器实现
 */
public class MVCCRecordManagerImpl implements MVCCRecordManager {
    // 原始记录管理器
    private final RecordManager recordManager;

    // 版本存储
    private final VersionStore versionStore;

    // 事务管理器
    private final MVCCTransactionManager txManager;

    // 锁
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 创建MVCC记录管理器
     * @param recordManager 基础记录管理器
     * @param versionStore 版本存储
     * @param txManager 事务管理器
     */
    public MVCCRecordManagerImpl(RecordManager recordManager, VersionStore versionStore, MVCCTransactionManager txManager) {
        this.recordManager = recordManager;
        this.versionStore = versionStore;
        this.txManager = txManager;
    }

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
        return versionStore.getVersionChain(rid);
    }

    @Override
    public RecordVersion getRecordVersion(RecordID rid, long timestamp) throws Exception {
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
        return versionStore.purgeOldVersions(olderThan);
    }

    // 实现RecordManager接口的方法，大部分委托给原始记录管理器

    @Override
    public RecordID insertRecord(byte[] data, long xid) throws Exception {
        return recordManager.insertRecord(data, xid);
    }

    @Override
    public boolean deleteRecord(RecordID rid, long xid) throws Exception {
        // 对于MVCC，删除操作创建一个带删除标记的新版本，而不是物理删除
        long beginTS = txManager.getBeginTimestamp(xid);
        return deleteRecordVersion(rid, xid, beginTS);
    }

    @Override
    public boolean updateRecord(RecordID rid, byte[] newData, long xid) throws Exception {
        // 对于MVCC，更新操作创建一个新版本，而不是覆盖
        long beginTS = txManager.getBeginTimestamp(xid);
        return updateRecordVersion(rid, newData, xid, beginTS);
    }

    @Override
    public byte[] getRecord(RecordID rid, long xid) throws Exception {
        // 为当前事务创建ReadView
        ReadView readView = txManager.createReadView(xid);

        // 使用MVCC可见性规则获取记录
        return getRecordWithMVCC(rid, readView);
    }

    @Override
    public Iterator<Record> scanRecords(Predicate<Record> predicate, long xid) throws Exception {
        // 获取当前事务的ReadView
        ReadView readView = txManager.createReadView(xid);

        // 创建结果列表
        List<Record> visibleRecords = new ArrayList<>();

        // 使用基础记录管理器获取所有记录
        Iterator<Record> allRecords = recordManager.scanRecords(null, xid);

        // 遍历所有记录
        while (allRecords.hasNext()) {
            Record baseRecord = allRecords.next();
            RecordID rid = baseRecord.getRid();

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

        return visibleRecords.iterator();
    }
}
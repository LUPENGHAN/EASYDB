package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Impl;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordHeader;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordVersion;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.RecordManager;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.VersionStore;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 版本存储实现类
 */
public class VersionStoreImpl implements VersionStore {
    // 页面管理器
    private final PageManager pageManager;

    // 记录管理器
    private final RecordManager recordManager;

    // 缓存最新版本指针 (记录ID -> 版本指针)
    private final Map<RecordID, Long> latestVersionPointers;

    // 读写锁，用于并发控制
    private final ReadWriteLock lock;

    /**
     * 创建版本存储
     * @param pageManager 页面管理器
     * @param recordManager 记录管理器
     */
    public VersionStoreImpl(PageManager pageManager, RecordManager recordManager) {
        this.pageManager = pageManager;
        this.recordManager = recordManager;
        this.latestVersionPointers = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    public boolean addVersion(RecordVersion version) throws Exception {
        lock.writeLock().lock();
        try {
            RecordID recordID = version.getRecordID();

            // 检查是否有最新版本
            if (latestVersionPointers.containsKey(recordID)) {
                // 获取最新版本
                long latestPointer = latestVersionPointers.get(recordID);
                RecordVersion latestVersion = getVersion(latestPointer);

                // 设置新版本的前一版本指针
                version.setPrevVersionPointer(latestPointer);

                // 更新最新版本的结束时间戳
                updateVersionEndTS(latestPointer, version.getBeginTS());
            }

            // 插入新版本记录
            byte[] recordData = version.toRecord();
            RecordID newVersionID = recordManager.insertRecord(recordData, version.getXid());

            // 更新最新版本指针缓存
            // 注意：在实际实现中，版本指针应该是一个特殊的标识，而不仅仅是RecordID
            // 这里简化为使用RecordID的哈希值作为版本指针
            long versionPointer = newVersionID.hashCode();
            latestVersionPointers.put(recordID, versionPointer);

            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public RecordVersion getLatestVersion(RecordID recordID) throws Exception {
        lock.readLock().lock();
        try {
            if (!latestVersionPointers.containsKey(recordID)) {
                return null;
            }

            long versionPointer = latestVersionPointers.get(recordID);
            return getVersion(versionPointer);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public RecordVersion getVersion(long versionPointer) throws Exception {
        // 在实际实现中，需要从版本指针解析出实际的RecordID
        // 这里简化处理，假设可以直接从版本指针得到RecordID
        RecordID versionID = getRecordIDFromPointer(versionPointer);

        // 读取记录数据
        byte[] recordData = recordManager.getRecord(versionID, 0); // 使用系统事务ID(0)读取
        if (recordData == null) {
            return null;
        }

        return RecordVersion.fromRecord(versionID, recordData);
    }

    @Override
    public List<RecordVersion> getVersionChain(RecordID recordID) throws Exception {
        lock.readLock().lock();
        try {
            List<RecordVersion> versionChain = new ArrayList<>();

            if (!latestVersionPointers.containsKey(recordID)) {
                return versionChain;
            }

            // 从最新版本开始，沿着版本链向前遍历
            long versionPointer = latestVersionPointers.get(recordID);
            while (versionPointer != RecordHeader.NULL_POINTER) {
                RecordVersion version = getVersion(versionPointer);
                if (version == null) {
                    break;
                }

                versionChain.add(version);
                versionPointer = version.getPrevVersionPointer();
            }

            return versionChain;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean updateVersionEndTS(long versionPointer, long endTS) throws Exception {
        lock.writeLock().lock();
        try {
            RecordID versionID = getRecordIDFromPointer(versionPointer);

            // 读取版本数据
            byte[] recordData = recordManager.getRecord(versionID, 0); // 使用系统事务ID(0)读取
            if (recordData == null) {
                return false;
            }

            RecordVersion version = RecordVersion.fromRecord(versionID, recordData);

            // 更新结束时间戳
            version.setEndTS(endTS);

            // 写回记录
            byte[] updatedRecordData = version.toRecord();
            return recordManager.updateRecord(versionID, updatedRecordData, 0); // 使用系统事务ID(0)更新
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int purgeOldVersions(long olderThan) throws Exception {
        // 实现过期版本清理
        // 这里需要遍历所有记录，查找并删除过期版本
        // 在实际实现中，可能需要更高效的索引机制来跟踪过期版本

        // 简化实现，返回清理的版本数量
        return 0;
    }

    /**
     * 从版本指针获取RecordID（实际实现需要根据存储格式设计）
     * @param versionPointer 版本指针
     * @return RecordID
     */
    private RecordID getRecordIDFromPointer(long versionPointer) {
        // 简化实现，实际应该根据版本存储格式来解析
        // 这里仅作示例
        return null;
    }
}
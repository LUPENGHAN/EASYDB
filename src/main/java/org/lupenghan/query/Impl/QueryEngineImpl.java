package org.lupenghan.query.Impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.lupenghan.eazydb.lock.models.Lock;
import org.lupenghan.eazydb.lock.models.LockType;
import org.lupenghan.eazydb.page.interfaces.PageManager;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.record.interfaces.RecordManager;
import org.lupenghan.eazydb.table.interfaces.TableManager;
import org.lupenghan.eazydb.table.models.Column;
import org.lupenghan.eazydb.table.models.ForeignKey;
import org.lupenghan.eazydb.table.models.Table;
import org.lupenghan.eazydb.transaction.interfaces.TransactionManager;
import org.lupenghan.query.interfaces.QueryEngine;
import org.lupenghan.eazydb.record.models.Record;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class QueryEngineImpl implements QueryEngine {
    @Getter
    private final TableManager tableManager;
    private final PageManager pageManager;
    private final RecordManager recordManager;
    private final TransactionManager transactionManager;

    public QueryEngineImpl(TableManager tableManager, PageManager pageManager, RecordManager recordManager, TransactionManager transactionManager) {
        this.tableManager = tableManager;
        this.pageManager = pageManager;
        this.recordManager = recordManager;
        this.transactionManager = transactionManager;
    }
    @Override
    public void createTable(Table table) throws IOException {
        if (tableManager.getTable(table.getName()) != null) {
            throw new IllegalArgumentException("表已存在: " + table.getName());
        }

        table.setCreatedTime(System.currentTimeMillis());
        table.setLastModifiedTime(System.currentTimeMillis());
        tableManager.createTable(table);

        log.info("✅ 表 {} 创建成功，共 {} 列", table.getName(), table.getColumns().size());
    }


    @Override
    public boolean dropTable(String tableName) throws IOException {
        log.info("执行 DROP TABLE {}", tableName);

        boolean success = tableManager.dropTable(tableName);

        if (!success) {
            throw new IOException("表删除失败，可能不存在: " + tableName);
        }

        log.info("表 {} 删除成功", tableName);
        return success;
    }



    @Override
    public long beginTransaction() {
        log.info("开始新事务");
        return transactionManager.begin();
    }

    @Override
    public void commitTransaction(long xid) throws IOException {
        log.info("提交事务: {}", xid);
        transactionManager.commit(xid);
    }

    @Override
    public void rollbackTransaction(long xid) throws IOException {
        log.info("回滚事务: {}", xid);
        transactionManager.rollback(xid);
    }

    @Override
    public void insert(long xid, String tableName, byte[] data) throws IOException {
        log.info("事务 {} 插入记录到表 {}", xid, tableName);

        Table table = tableManager.getTable(tableName);
        if (table == null) throw new IllegalArgumentException("表不存在：" + tableName);

        // 创建新页并获取页级排他锁
        Page page = pageManager.createPage();
        Lock pageLock = acquirePageLock(xid, page, LockType.EXCLUSIVE_LOCK);
        
        if (pageLock == null) {
            log.error("事务 {} 无法获取页 {} 的排他锁，可能发生死锁", xid, page.getHeader().getPageId());
            rollbackTransaction(xid);
            throw new RuntimeException("无法获取锁，事务 " + xid + " 已回滚");
        }
        
        try {
            recordManager.insert(page, data, xid);
        } catch (Exception e) {
            log.error("事务 {} 插入记录失败", xid, e);
            rollbackTransaction(xid);
            throw e;
        }
        for (ForeignKey fk : table.getForeignKeys()) {
            // 查找对应字段
            int fieldIndex = -1;
            for (int i = 0; i < table.getColumns().size(); i++) {
                if (table.getColumns().get(i).getName().equals(fk.getColumnName())) {
                    fieldIndex = i;
                    break;
                }
            }
            if (fieldIndex == -1) continue;

            // 提取该字段数据
            Column col = table.getColumns().get(fieldIndex);
            int offset = 0;
            for (int i = 0; i < fieldIndex; i++) {
                offset += table.getColumns().get(i).getLength();
            }
            byte[] fieldValue = new byte[col.getLength()];
            System.arraycopy(data, offset, fieldValue, 0, col.getLength());

            // 验证外键值是否存在
            Table referencedTable = tableManager.getTable(fk.getReferencedTable());
            if (referencedTable == null) throw new RuntimeException("外键引用表不存在: " + fk.getReferencedTable());

            boolean found = false;
            for (int i = 1; i <= pageManager.getTotalPages(); i++) {
                Page p = pageManager.readPage(i);
                for (Record r : recordManager.getAllRecords(p)) {
                    byte[] candidate = r.getData();
                    // 假设引用字段在开头，等长匹配
                    if (startsWith(candidate, fieldValue)) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) throw new RuntimeException("外键值不存在: " + new String(fieldValue));
        }
    }
    private boolean startsWith(byte[] full, byte[] prefix) {
        if (full.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (full[i] != prefix[i]) return false;
        }
        return true;
    }
    @Override
    public void update(long xid, String tableName, int pageId, int slotId, byte[] newData) throws IOException {
        log.info("事务 {} 更新表 {} 中页 {} 的槽位 {}", xid, tableName, pageId, slotId);
        
        Page page = pageManager.readPage(pageId);
        
        // 尝试获取记录级排他锁
        Lock recordLock = acquireRecordLock(xid, page, slotId, LockType.EXCLUSIVE_LOCK);
        
        if (recordLock == null) {
            log.error("事务 {} 无法获取记录锁 ({},{}), 可能发生死锁", xid, pageId, slotId);
            rollbackTransaction(xid);
            throw new RuntimeException("无法获取锁，事务 " + xid + " 已回滚");
        }
        
        try {
            Record record = page.getRecords().get(slotId);
            if (record == null) {
                throw new IllegalArgumentException("记录不存在，页 " + pageId + " 槽位 " + slotId);
            }
            recordManager.update(page, record, newData, xid);
        } catch (Exception e) {
            log.error("事务 {} 更新记录失败", xid, e);
            rollbackTransaction(xid);
            throw e;
        }
    }

    @Override
    public void delete(long xid, String tableName, int pageId, int slotId) throws IOException {
        log.info("事务 {} 删除表 {} 中页 {} 的槽位 {}", xid, tableName, pageId, slotId);
        
        Page page = pageManager.readPage(pageId);
        
        // 尝试获取记录级排他锁
        Lock recordLock = acquireRecordLock(xid, page, slotId, LockType.EXCLUSIVE_LOCK);
        
        if (recordLock == null) {
            log.error("事务 {} 无法获取记录锁 ({},{}), 可能发生死锁", xid, pageId, slotId);
            rollbackTransaction(xid);
            throw new RuntimeException("无法获取锁，事务 " + xid + " 已回滚");
        }
        
        try {
            Record record = page.getRecords().get(slotId);
            if (record == null) {
                throw new IllegalArgumentException("记录不存在，页 " + pageId + " 槽位 " + slotId);
            }
            recordManager.delete(page, record, xid);
        } catch (Exception e) {
            log.error("事务 {} 删除记录失败", xid, e);
            rollbackTransaction(xid);
            throw e;
        }
    }

    @Override
    public byte[] select(String tableName, int pageId, int slotId) throws IOException {
        log.info("查询表 {} 中页 {} 的槽位 {}", tableName, pageId, slotId);
        
        // 开始一个读事务
        long readXid = beginTransaction();
        
        Page page = pageManager.readPage(pageId);
        
        // 尝试获取记录级共享锁
        Lock recordLock = acquireRecordLock(readXid, page, slotId, LockType.SHARED_LOCK);
        
        if (recordLock == null) {
            log.error("无法获取记录的共享锁 ({},{})", pageId, slotId);
            rollbackTransaction(readXid);
            throw new RuntimeException("无法获取共享锁，读操作失败");
        }
        
        try {
            Record record = page.getRecords().get(slotId);
            if (record == null) {
                throw new IllegalArgumentException("记录不存在，页 " + pageId + " 槽位 " + slotId);
            }
            byte[] result = recordManager.select(page, record);
            
            // 查询完成后提交读事务
            commitTransaction(readXid);
            
            return result;
        } catch (Exception e) {
            log.error("查询记录失败", e);
            rollbackTransaction(readXid);
            throw e;
        }
    }

    @Override
    public List<byte[]> selectAll(String tableName) throws IOException {
        log.info("查询表 {} 的所有记录", tableName);

        // 开始一个读事务
        long readXid = beginTransaction();
        List<byte[]> result = new ArrayList<>();
        List<Lock> acquiredLocks = new ArrayList<>();

        try {
            for (int i = 1; i <= pageManager.getTotalPages(); i++) {
                Page page = pageManager.readPage(i);

                // 尝试获取页级共享锁
                Lock pageLock = acquirePageLock(readXid, page, LockType.SHARED_LOCK);

                if (pageLock == null) {
                    log.error("无法获取页 {} 的共享锁", i);
                    // 继续处理其他页，但记录错误
                    continue;
                }

                acquiredLocks.add(pageLock);

                for (Record record : recordManager.getAllRecords(page)) {
                    result.add(record.getData());
                }
            }

            // 查询完成后提交读事务
            commitTransaction(readXid);
            return result;
        } catch (Exception e) {
            log.error("查询所有记录失败", e);
            rollbackTransaction(readXid);
            throw e;
        }
    }

    // 私有辅助方法：获取页级锁
    private Lock acquirePageLock(long xid, Page page, LockType lockType) {
        int pageId = page.getHeader().getPageId();
        return transactionManager.acquireLock(xid, page, lockType);
    }
    
    // 私有辅助方法：获取记录级锁
    private Lock acquireRecordLock(long xid, Page page, int slotId, LockType lockType) {
        // 直接使用LockManager获取记录级锁，而不是通过TransactionManager
        // 这样可以准确指定记录的slotId
        int pageId = page.getHeader().getPageId();
        Lock lock = null;
        
        try {
            // 获取记录级锁
            lock = transactionManager.getLockManager().acquireLock(xid, lockType, pageId, slotId);
            
            if (lock != null && lockType == LockType.EXCLUSIVE_LOCK) {
                // 如果是写锁，记录修改的页面和记录
                Record record = page.getRecords().get(slotId);
                if (record != null) {
                    // 将修改过的页面添加到事务的修改页面列表中
                    transactionManager.getModifiedPages(xid).add(page);
                }
            }
        } catch (Exception e) {
            log.error("获取记录锁失败: xid={}, pageId={}, slotId={}, lockType={}", 
                     xid, pageId, slotId, lockType, e);
        }
        
        return lock;
    }
}

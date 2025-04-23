package org.lupenghan.query.Impl;

import lombok.extern.slf4j.Slf4j;
import org.lupenghan.eazydb.page.interfaces.PageManager;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.record.interfaces.RecordManager;
import org.lupenghan.eazydb.table.interfaces.TableManager;
import org.lupenghan.eazydb.transaction.interfaces.TransactionManager;
import org.lupenghan.query.interfaces.QueryEngine;
import org.lupenghan.eazydb.record.models.Record;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class QueryEngineImpl implements QueryEngine {
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
    public long beginTransaction() {
        return transactionManager.begin();
    }

    @Override
    public void commitTransaction(long xid) throws IOException {
        transactionManager.commit(xid);
    }

    @Override
    public void rollbackTransaction(long xid) throws IOException {
        transactionManager.rollback(xid);
    }

    @Override
    public void insert(long xid, String tableName, byte[] data) throws IOException {
        // 目前简化：始终写入新页
        Page page = pageManager.createPage();
        recordManager.insert(page, data, xid);
    }

    @Override
    public void update(long xid, String tableName, int pageId, int slotId, byte[] newData) throws IOException {
        Page page = pageManager.readPage(pageId);
        Record record = page.getRecords().get(slotId);
        recordManager.update(page, record, newData, xid);
    }

    @Override
    public void delete(long xid, String tableName, int pageId, int slotId) throws IOException {
        Page page = pageManager.readPage(pageId);
        Record record = page.getRecords().get(slotId);
        recordManager.delete(page, record, xid);
    }

    @Override
    public byte[] select(String tableName, int pageId, int slotId) throws IOException {
        Page page = pageManager.readPage(pageId);
        Record record = page.getRecords().get(slotId);
        return recordManager.select(page, record);
    }

    @Override
    public List<byte[]> selectAll(String tableName) throws IOException {
        List<byte[]> result = new ArrayList<>();
        for (int i = 1; i <= pageManager.getTotalPages(); i++) {
            Page page = pageManager.readPage(i);
            for (Record record : recordManager.getAllRecords(page)) {
                result.add(record.getData());
            }
        }
        return result;
    }
}

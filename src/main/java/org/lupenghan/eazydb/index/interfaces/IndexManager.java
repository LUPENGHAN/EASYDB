package org.lupenghan.eazydb.index.interfaces;

import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.transaction.interfaces.TransactionManager;

import java.util.List;

public interface IndexManager {
    void createIndex(String tableName, String columnName, String indexName, TransactionManager transactionManager);
    void dropIndex(String indexName, TransactionManager transactionManager);
    void insertIndex(String indexName, Object key, Record record, TransactionManager transactionManager);
    void deleteIndex(String indexName, Object key, Record record, TransactionManager transactionManager);
    List<Record> rangeQuery(String indexName, Object minKey, Object maxKey, TransactionManager transactionManager);
    List<Record> exactQuery(String indexName, Object key, TransactionManager transactionManager);
    Page getIndexRoot(String indexName);
    void updateIndex(String indexName, Object oldKey, Object newKey, Record record, TransactionManager transactionManager);

}

package org.lupenghan.query.interfaces;

import java.io.IOException;
import java.util.List;

import org.lupenghan.eazydb.table.interfaces.TableManager;
import org.lupenghan.eazydb.table.models.Column;
import org.lupenghan.eazydb.table.models.Table;


public interface QueryEngine {
    long beginTransaction();

    void commitTransaction(long xid) throws IOException;

    void rollbackTransaction(long xid) throws IOException;

    void insert(long xid, String tableName, byte[] data) throws IOException;

    void update(long xid, String tableName, int pageId, int slotId, byte[] newData) throws IOException;

    void delete(long xid, String tableName, int pageId, int slotId) throws IOException;

    byte[] select(String tableName, int pageId, int slotId) throws IOException;

    List<byte[]> selectAll(String tableName) throws IOException;

    void createTable(Table table) throws IOException;

    boolean dropTable(String tableName) throws IOException;

    TableManager getTableManager();


}


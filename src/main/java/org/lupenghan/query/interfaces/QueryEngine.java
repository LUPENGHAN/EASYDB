package org.lupenghan.query.interfaces;

import java.io.IOException;
import java.util.List;
import org.lupenghan.eazydb.record.models.Record;


public interface QueryEngine {
    long beginTransaction();

    void commitTransaction(long xid) throws IOException;

    void rollbackTransaction(long xid) throws IOException;

    void insert(long xid, String tableName, byte[] data) throws IOException;

    void update(long xid, String tableName, int pageId, int slotId, byte[] newData) throws IOException;

    void delete(long xid, String tableName, int pageId, int slotId) throws IOException;

    byte[] select(String tableName, int pageId, int slotId) throws IOException;

    List<byte[]> selectAll(String tableName) throws IOException;
}


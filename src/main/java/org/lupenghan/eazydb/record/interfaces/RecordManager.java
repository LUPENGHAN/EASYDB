package org.lupenghan.eazydb.record.interfaces;

import org.lupenghan.eazydb.log.models.LogRecord;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.record.models.Record;

import java.io.IOException;
import java.util.List;

public interface RecordManager {
    org.lupenghan.eazydb.record.models.Record insert(Page page, byte[] data, long xid) throws IOException;
    Record update(Page page, Record record, byte[] newData, long xid) throws IOException;
    void delete(Page page, Record record, long xid) throws IOException;
    byte[] select(Page page, Record record);
    List<Record> getAllRecords(Page page);
    void rollbackRecord(Page page, LogRecord log);
    boolean isValidRecord(Record record);


}

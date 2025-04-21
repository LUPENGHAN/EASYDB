package org.lupenghan.eazydb.record.interfaces;

import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.record.models.Record;
import java.util.List;

public interface RecordManager {
    org.lupenghan.eazydb.record.models.Record insert(Page page, byte[] data, long xid);
    Record update(Page page, Record record, byte[] newData, long xid);
    void delete(Page page, Record record, long xid);
    byte[] select(Page page, Record record);
    List<Record> getAllRecords(Page page);

    boolean isValidRecord(Record record);


}

package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement;

import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.Record;
import org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform.RecordID;

import java.util.Iterator;
import java.util.function.Predicate;

public interface RecordManager {
    // 插入记录，返回记录ID
    RecordID insertRecord(byte[] data, long xid) throws Exception;

    // 删除记录
    boolean deleteRecord(RecordID rid, long xid) throws Exception;

    // 更新记录
    boolean updateRecord(RecordID rid, byte[] newData, long xid) throws Exception;

    // 获取记录
    byte[] getRecord(RecordID rid, long xid) throws Exception;

    // 扫描记录（修改为具体的泛型类型）
    Iterator<Record> scanRecords(Predicate<Record> predicate, long xid) throws Exception;
}
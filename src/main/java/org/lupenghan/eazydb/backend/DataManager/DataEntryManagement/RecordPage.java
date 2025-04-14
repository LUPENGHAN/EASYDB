package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement;

import java.util.Iterator;

public interface RecordPage {
    // 在页面中插入记录
    int insertRecord(byte[] data) throws Exception;

    // 从页面中删除记录
    boolean deleteRecord(int slotNum) throws Exception;

    // 更新页面中的记录
    boolean updateRecord(int slotNum, byte[] newData) throws Exception;

    // 获取页面中的记录
    byte[] getRecord(int slotNum) throws Exception;

    // 获取页面剩余空间
    int getFreeSpace();

    // 整理页面（移除碎片）
    void compact();

    // 获取所有有效记录的槽号
    Iterator<Integer> getValidSlots();
}
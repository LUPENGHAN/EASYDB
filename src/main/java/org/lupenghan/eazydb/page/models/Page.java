package org.lupenghan.eazydb.page.models;
import org.lupenghan.eazydb.record.models.Record;

import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Page {
    private static final int PAGE_SIZE = 4096; // 4KB页面大小
    private static final int PAGE_HEADER_SIZE = 64; // 你 PageHead 占用的总字节数
    private static final int SLOT_ENTRY_SIZE = 24;  // 每个 SlotDirectoryEntry 占用的大小
    private int nextFreeOffsetFromEnd = PAGE_SIZE;// 初始从页尾开始分配
    // B+树索引相关字段
    private List<Object> keys;      // 键列表
    private List<Page> children;    // 子页面列表
    private List<Record> records;   // 记录列表（仅叶子节点使用）

    // 页内主要结构
    private PageHead header;
    private List<SlotDirectoryEntry> slotDirectory;
    private byte[] freeSpace; // 用于表示空闲区域，也可抽象为 FreeSpaceManager

    // 页面状态
    private boolean isDirty;
    private int pinCount;
    private long lastAccessTime;

    public Page(int pageId) {
        this.header = PageHead.builder()
                .pageId(pageId)
                .createTime(System.currentTimeMillis())
                .lastModifiedTime(System.currentTimeMillis())
                .pageType(PageType.DATA.getValue())
                .slotCount(0)
                .recordCount(0)
                .isLeaf(true)
                .keyCount(0)
                .build();

        this.slotDirectory = new ArrayList<>();
        this.freeSpace = new byte[0];
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        this.records = new ArrayList<>();

        this.isDirty = false;
        this.pinCount = 0;
        this.lastAccessTime = System.currentTimeMillis();
    }

    //pin 相关
    public void pin() {
        pinCount++;
    }

    public void unpin() {
        if (pinCount > 0) {
            pinCount--;
        }
    }
    //更新最后访问时间
    public void updateLastAccessTime() {
        lastAccessTime = System.currentTimeMillis();
    }
    public int allocateRecordSpace(int size) {
        int slotEnd =  PAGE_HEADER_SIZE + (getSlotDirectory().size() * SLOT_ENTRY_SIZE);
        if (nextFreeOffsetFromEnd - size < slotEnd) {
            return -1; // 空间不足
        }
        nextFreeOffsetFromEnd -= size;
        return nextFreeOffsetFromEnd;
    }


    /**
     * 添加槽位目录项
     */
    public void addSlotDirectoryEntry(SlotDirectoryEntry entry) {
        slotDirectory.add(entry);
        header.setSlotCount(header.getSlotCount() + 1);
        isDirty = true;
    }


    // B+树索引相关方法
//    public void addKey(Object key) {
//        keys.add(key);
//        header.setKeyCount(header.getKeyCount() + 1);
//        isDirty = true;
//    }
//
//    public void addKey(int index, Object key) {
//        keys.add(index, key);
//        header.setKeyCount(header.getKeyCount() + 1);
//        isDirty = true;
//    }
//
//    public void addChild(Page child) {
//        children.add(child);
//        isDirty = true;
//    }
//
//    public void addChild(int index, Page child) {
//        children.add(index, child);
//        isDirty = true;
//    }
//
//    public void addRecord(Record record) {
//        records.add(record);
//        header.setRecordCount(header.getRecordCount() + 1);
//        isDirty = true;
//    }
//
//    public void addRecord(int index, Record record) {
//        records.add(index, record);
//        header.setRecordCount(header.getRecordCount() + 1);
//        isDirty = true;
//    }
//
//    public Object getKey(int index) {
//        return keys.get(index);
//    }
//
//    public Page getChild(int index) {
//        return children.get(index);
//    }
//
//    public Record getRecord(int index) {
//        return records.get(index);
//    }
//
//    public void setKey(int index, Object key) {
//        keys.set(index, key);
//        isDirty = true;
//    }
//
//    public void setChild(int index, Page child) {
//        children.set(index, child);
//        isDirty = true;
//    }
//
//    public void setRecord(int index, Record record) {
//        records.set(index, record);
//        isDirty = true;
//    }
//
//    public void removeKey(int index) {
//        keys.remove(index);
//        header.setKeyCount(header.getKeyCount() - 1);
//        isDirty = true;
//    }
//
//    public void removeChild(int index) {
//        children.remove(index);
//        isDirty = true;
//    }
//
//    public void removeRecord(int index) {
//        records.remove(index);
//        header.setRecordCount(header.getRecordCount() - 1);
//        isDirty = true;
//    }


}

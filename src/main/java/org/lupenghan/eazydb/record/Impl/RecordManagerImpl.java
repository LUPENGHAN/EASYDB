package org.lupenghan.eazydb.record.Impl;

import lombok.Data;
import org.lupenghan.eazydb.page.interfaces.PageManager;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.page.models.SlotDirectoryEntry;
import org.lupenghan.eazydb.record.interfaces.RecordManager;
import org.lupenghan.eazydb.record.models.Record;

import java.util.ArrayList;
import java.util.List;

import static org.lupenghan.eazydb.record.models.RecordStatus.*;

@Data
public class RecordManagerImpl implements RecordManager {
    private final PageManager pageManager;
    private final byte ACTIVE = 0;
    private final byte DELETED = 1;
    private final byte UPDATED = 2;


    @Override
    public Record insert(Page page, byte[] data, long xid) {
        // 简化：假设单字段，无 NULL
        byte[] nullBitmap = new byte[1];
        short[] fieldOffsets = new short[] {0};
        //不简化
         // N 个字段需要 (N + 7) / 8 个字节的 nullBitmap
//        byte[] nullBitmap = new byte[(fieldCount + 7) / 8];
//
//        // 设置第 i 个字段为 NULL（如果有）
//        if (isFieldNull(i)) {
//            nullBitmap[i / 8] |= (1 << (i % 8));
//        }
//        short[] fieldOffsets = new short[fieldCount];
//        int offsetInData = 0;
//
//        for (int i = 0; i < fieldCount; i++) {
//            fieldOffsets[i] = (short) offsetInData;
//            if (!isFieldNull(i)) {
//                offsetInData += getFieldLength(i);  // 字段的实际长度
//            }
//        }

        int baseMetadataSize = 4 + 1 + 8 + 8 + 8 + 8 + 4 + 4; // length + status + xid + beginTS + endTS + ptr + pageId + slotId
        int nullBitmapSize = nullBitmap.length;
        int fieldOffsetSize = fieldOffsets.length * 2;
        int totalRecordSize = baseMetadataSize + nullBitmapSize + fieldOffsetSize + data.length;

        int offset = page.allocateRecordSpace(totalRecordSize);

        if (offset == -1) return null;

        Record record = new Record();
        record.setLength( totalRecordSize);
        record.setStatus(ACTIVE);
        record.setXid(xid);
        record.setBeginTS(System.currentTimeMillis());
        record.setEndTS(Long.MAX_VALUE);
        record.setPrevVersionPointer(-1);
        record.setData(data);
        record.setNullBitmap(nullBitmap);
        record.setFieldOffsets(fieldOffsets);

        // 插入逻辑：包含槽位管理
        int slotId = findReusableSlot(page);
        if (slotId == -1) {
            slotId = page.getSlotDirectory().size();
            page.getSlotDirectory().add(null); // 占位
            page.getHeader().setSlotCount(page.getHeader().getSlotCount() + 1);
        }

        record.setPageId(page.getHeader().getPageId());
        record.setSlotId(slotId);
        page.getRecords().add(record);

        SlotDirectoryEntry slot = SlotDirectoryEntry.builder()
                .offset(offset)
                .inUse(true)
                .build();

        page.getSlotDirectory().set(slotId, slot);
        page.getHeader().setRecordCount(page.getHeader().getRecordCount() + 1);
        page.setDirty(true);

        return record;
    }

    @Override
    public Record update(Page page, Record record, byte[] newData, long xid) {
        record.setStatus(UPDATED);
        record.setEndTS(System.currentTimeMillis());

        Record newRecord = new Record();
        newRecord.setStatus(ACTIVE);
        newRecord.setXid(xid);
        newRecord.setBeginTS(System.currentTimeMillis());
        newRecord.setEndTS(Long.MAX_VALUE);
        newRecord.setPrevVersionPointer(record.getSlotId());
        newRecord.setData(newData);
        newRecord.setNullBitmap(record.getNullBitmap().clone());
        newRecord.setFieldOffsets(record.getFieldOffsets().clone());

        return insert(page, newRecord.getData(), xid);
    }

    @Override
    public void delete(Page page, Record record, long xid) {
        record.setStatus(DELETED);
        record.setEndTS(System.currentTimeMillis());
        record.setXid(xid);
        SlotDirectoryEntry slot = page.getSlotDirectory().get(record.getSlotId());
        if (slot != null) slot.setInUse(false);
        page.getHeader().setRecordCount(page.getHeader().getRecordCount() - 1);
        page.setDirty(true);
    }


    @Override
    public byte[] select(Page page, Record record) {
        if (!isValidRecord(record)) {
            return null;
        }
        return record.getData();
    }

    @Override
    public List<Record> getAllRecords(Page page) {
        List<Record> validRecords = new ArrayList<>();
        for (Record record : page.getRecords()) {
            if (isValidRecord(record)) {
                validRecords.add(record);
            }
        }
        return validRecords;
    }



    @Override
    public boolean isValidRecord(Record record) {
        return record != null &&
                record.getStatus() == ACTIVE &&
                System.currentTimeMillis() >= record.getBeginTS() &&
                System.currentTimeMillis() < record.getEndTS();
    }
    // 寻找可复用的槽位
    private static int findReusableSlot(Page page) {
        for (int i = 0; i < page.getSlotDirectory().size(); i++) {
            if (!page.getSlotDirectory().get(i).isInUse()) {
                return i;
            }
        }
        return -1;
    }
}

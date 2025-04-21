package org.lupenghan.eazydb.page.Impl;

import lombok.Data;
import org.lupenghan.eazydb.page.interfaces.PageManager;
import org.lupenghan.eazydb.page.models.FreeSpaceEntry;
import org.lupenghan.eazydb.page.models.Page;
import org.lupenghan.eazydb.page.models.PageHead;
import org.lupenghan.eazydb.page.models.SlotDirectoryEntry;
import org.lupenghan.eazydb.record.Impl.RecordManagerImpl;
import org.lupenghan.eazydb.record.models.Record;
import org.lupenghan.eazydb.record.models.RecordStatus;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class PageManagerImpl implements PageManager {
    private final String dataFilePath;
    private final RandomAccessFile dataFile;
    private final Map<Integer, Page> pageCache;
    private final AtomicInteger nextPageId;
    private static final int MAX_CACHE_SIZE = 1000;
    private static final int PAGE_SIZE = 4096; // 4KB页面大小

    public PageManagerImpl(String dataFilePath) throws IOException {
        this.dataFilePath = dataFilePath;
        File file = new File(dataFilePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        this.dataFile = new RandomAccessFile(dataFilePath, "rw");
        this.pageCache = new HashMap<>();
        this.nextPageId = new AtomicInteger(0);
    }



    @Override
    public Page createPage() {
        int pageId = nextPageId.incrementAndGet();
        Page page = new Page(pageId);
        pageCache.put(pageId, page);
        return page;

    }

    @Override
    public Page readPage(int pageId) throws IOException {
        Page page = pageCache.get(pageId);
        if (page != null) {
            page.updateLastAccessTime();
            return page;
        }
        long offset = (long) pageId * PAGE_SIZE;
        if (offset >= dataFile.length()) {
            return null;
        }
        dataFile.seek(offset);
        byte[] pageData = new byte[PAGE_SIZE];
        dataFile.readFully(pageData);

        page = parsePageData(pageId, pageData);
        page.setDirty(false);

        // 更新缓存 简单实现LRU减少缓存
        if (pageCache.size() >= MAX_CACHE_SIZE) {
            evictPage();
        }
        pageCache.put(pageId, page);

        return page;
    }


    @Override
    public void writePage(Page page) throws IOException {
        if (!page.isDirty()) {
            return;
        }

        long offset = (long) page.getHeader().getPageId() * PAGE_SIZE;
        dataFile.seek(offset);
        byte[] pageData = serializePage(page);
        dataFile.write(pageData);
        page.setDirty(false);
    }

    @Override
    public int getTotalPages() {
        return nextPageId.get();
    }


    @Override
    public void setPageType(Page page, byte type) {
        page.getHeader().setPageType(type);
        page.setDirty(true);
        page.updateLastAccessTime();
    }


    //压缩页面并未实现
    @Override
    public void compactPage(Page page) {
        page.setDirty(true);
    }

    @Override
    public boolean needsCompaction(Page page) {
        // 如果碎片化程度超过50%，则需要压缩
//        int totalFreeSpace = page.getFreeSpaceDirectory().stream()
//                .mapToInt(FreeSpaceEntry::getLength)
//                .sum();
//        return totalFreeSpace > 0 &&
//                (double)totalFreeSpace / (page.getRecords().size() * 100) > 0.5;
        return false;
    }

    private Page parsePageData(int pageId, byte[] pageData) {
        ByteBuffer buffer = ByteBuffer.wrap(pageData);

        // 创建页面对象
        Page page = new Page(pageId);
        PageHead header = page.getHeader();

        // 解析页面头部
        header.setPageId(buffer.getInt());
        header.setFileOffset(buffer.getLong());
        header.setPageLSN(buffer.getLong());
        header.setPageType(buffer.get());

        header.setFreeSpacePointer(buffer.getShort());
        header.setSlotCount(buffer.getInt());
        header.setRecordCount(buffer.getInt());
        header.setChecksum(buffer.getInt());
        header.setVersion(buffer.getLong());
        header.setCreateTime(buffer.getLong());
        header.setLastModifiedTime(buffer.getLong());
        header.setLeaf(buffer.get()==1);
        header.setKeyCount(buffer.getInt());
        // 解析槽位目录
        int slotCount = header.getSlotCount();
        List<SlotDirectoryEntry> slotDirectory = new ArrayList<>(slotCount);
        for (int i = 0; i < slotCount; i++) {
            SlotDirectoryEntry entry = new SlotDirectoryEntry();
            entry.setOffset(buffer.getInt());
            entry.setInUse(buffer.get() == 1);
            entry.setReserved1(buffer.get());
            entry.setReserved2(buffer.getShort());
            slotDirectory.add(entry);
        }
        page.setSlotDirectory(slotDirectory);


        // 解析记录
        int recordCount = header.getRecordCount();
        List<Record> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            Record record = new Record();
            record.setLength(buffer.getShort());
            record.setStatus(buffer.get());
            record.setXid(buffer.getLong());
            record.setBeginTS(buffer.getLong());
            record.setEndTS(buffer.getLong());
            record.setPrevVersionPointer(buffer.getLong());
            record.setPageId(buffer.getInt());
            record.setSlotId(buffer.getInt());

//            // 读取null位图
//            int nullBitmapLength = (recordCount + 7) / 8;
//            byte[] nullBitmap = new byte[nullBitmapLength];
//            buffer.get(nullBitmap);
//            record.setNullBitmap(nullBitmap);
//
//            // 读取字段偏移
//            int fieldCount = buffer.getShort();
//            short[] fieldOffsets = new short[fieldCount];
//            for (int j = 0; j < fieldCount; j++) {
//                fieldOffsets[j] = buffer.getShort();
//            }
//            record.setFieldOffsets(fieldOffsets);
            record.setNullBitmap(new byte[]{buffer.get()});
            record.setFieldOffsets(new short[]{buffer.getShort()});
            // 读取记录数据
            byte[] data = new byte[record.getLength()- 45];
            buffer.get(data);
            record.setData(data);

            // 设置记录引用
            record.setPageId(pageId);
            record.setSlotId(i);

            records.add(record);
        }
        page.setRecords(records);

        return page;
    }
    private void evictPage() {
        // 简单的LRU策略
        long oldestTime = Long.MAX_VALUE;
        int pageIdToEvict = -1;

        for (Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            Page page = entry.getValue();
            if (page.getPinCount() == 0 && page.getLastAccessTime() < oldestTime) {
                oldestTime = page.getLastAccessTime();
                pageIdToEvict = entry.getKey();
            }
        }

        if (pageIdToEvict != -1) {
            Page page = pageCache.remove(pageIdToEvict);
            if (page.isDirty()) {
                try {
                    writePage(page);
                } catch (IOException e) {
                    // 处理写入失败的情况
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * 序列化页面数据
     */
    private byte[] serializePage(Page page) {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        PageHead header = page.getHeader();

        // 序列化页面头部
        buffer.putInt(header.getPageId());
        buffer.putLong(header.getFileOffset());
        buffer.putLong(header.getPageLSN());
        buffer.put(header.getPageType());
        buffer.putShort(header.getFreeSpacePointer());
        buffer.putInt(header.getSlotCount());
        buffer.putInt(header.getRecordCount());
        buffer.putInt(header.getChecksum());
        buffer.putLong(header.getVersion());
        buffer.putLong(header.getCreateTime());
        buffer.putLong(header.getLastModifiedTime());
        buffer.put((byte) (header.isLeaf() ? 1 : 0));
        buffer.putInt(header.getKeyCount());
        // 序列化槽位目录
        for (SlotDirectoryEntry entry : page.getSlotDirectory()) {
            buffer.putInt( entry.getOffset());
            buffer.put((byte)(entry.isInUse() ? 1 : 0));
            buffer.put( entry.getReserved1());
            buffer.putShort(entry.getReserved2());

        }


        // 序列化记录
        for (Record record : page.getRecords()) {
            buffer.putInt(record.getLength());
            buffer.put(record.getStatus());
            buffer.putLong(record.getXid());
            buffer.putLong(record.getBeginTS());
            buffer.putLong(record.getEndTS());
            buffer.putLong(record.getPrevVersionPointer());
            buffer.putInt(record.getPageId());
            buffer.putInt(record.getSlotId());

//            // 写入null位图（确保不为null）
//            byte[] nullBitmap = record.getNullBitmap();
//            if (nullBitmap == null) {
//                // 如果nullBitmap为null，创建一个空位图（全为0，表示没有NULL值）
//                int nullBitmapLength = (page.getRecords().size() + 7) / 8;
//                nullBitmap = new byte[nullBitmapLength > 0 ? nullBitmapLength : 1];
//            }
//            buffer.put(nullBitmap);
//
//            // 写入字段偏移（确保不为null）
//            short[] fieldOffsets = record.getFieldOffsets();
//            if (fieldOffsets == null) {
//                fieldOffsets = new short[0];
//            }
//            buffer.putShort((short)fieldOffsets.length);
//            for (short offset : fieldOffsets) {
//                buffer.putShort(offset);
//            }
            //暂时简化
            byte[] nullBitmap = new byte[1];
            short[] fieldOffsets = new short[] {0};
            buffer.put((byte) 1);
            buffer.putShort((short) 0);
            // 写入记录数据（确保不为null）
            byte[] data = record.getData();
            if (data == null) {
                // 如果数据为null，使用空数组
                data = new byte[0];
            }
            buffer.put(data);
        }

        // 填充剩余空间
        while (buffer.position() < PAGE_SIZE) {
            buffer.put((byte)0);
        }

        return buffer.array();
    }
}

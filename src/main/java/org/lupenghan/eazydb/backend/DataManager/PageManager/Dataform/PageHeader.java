package org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform;

import java.nio.ByteBuffer;

/**
 * 页面头部结构 - 定义页面元数据
 */
public class PageHeader {
    // 页面头部大小（字节）
    public static final int PAGE_HEADER_SIZE = 24;

    // 页面头部数据的偏移量
    private static final int PAGE_ID_OFFSET = 0;           // 页面ID (8字节)
    private static final int LSN_OFFSET = 8;               // 日志序列号 (8字节)
    private static final int FREE_SPACE_OFFSET = 16;       // 空闲空间指针 (2字节)
    private static final int RECORD_COUNT_OFFSET = 18;     // 记录数量 (2字节)
    private static final int CHECKSUM_OFFSET = 20;         // 校验和 (4字节)

    private final byte[] headerData;

    public PageHeader() {
        headerData = new byte[PAGE_HEADER_SIZE];
    }

    public PageHeader(byte[] data) {
        if (data.length < PAGE_HEADER_SIZE) {
            throw new IllegalArgumentException("数据长度不足以包含页面头部");
        }
        this.headerData = new byte[PAGE_HEADER_SIZE];
        System.arraycopy(data, 0, headerData, 0, PAGE_HEADER_SIZE);
    }

    // 获取页面ID
    public PageID getPageID() {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, PAGE_ID_OFFSET, 8);
        int fileID = buffer.getInt();
        int pageNum = buffer.getInt();
        return new PageID(fileID, pageNum);
    }

    // 设置页面ID
    public void setPageID(PageID pageID) {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, PAGE_ID_OFFSET, 8);
        buffer.putInt(pageID.getFileID());
        buffer.putInt(pageID.getPageNum());
    }

    // 获取LSN
    public long getLSN() {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, LSN_OFFSET, 8);
        return buffer.getLong();
    }

    // 设置LSN
    public void setLSN(long lsn) {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, LSN_OFFSET, 8);
        buffer.putLong(lsn);
    }

    // 获取空闲空间指针
    public short getFreeSpacePointer() {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, FREE_SPACE_OFFSET, 2);
        return buffer.getShort();
    }

    // 设置空闲空间指针
    public void setFreeSpacePointer(short pointer) {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, FREE_SPACE_OFFSET, 2);
        buffer.putShort(pointer);
    }

    // 获取记录数量
    public short getRecordCount() {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, RECORD_COUNT_OFFSET, 2);
        return buffer.getShort();
    }

    // 设置记录数量
    public void setRecordCount(short count) {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, RECORD_COUNT_OFFSET, 2);
        buffer.putShort(count);
    }

    // 获取校验和
    public int getChecksum() {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, CHECKSUM_OFFSET, 4);
        return buffer.getInt();
    }

    // 设置校验和
    public void setChecksum(int checksum) {
        ByteBuffer buffer = ByteBuffer.wrap(headerData, CHECKSUM_OFFSET, 4);
        buffer.putInt(checksum);
    }

    // 计算当前头部的校验和
    public int calculateChecksum() {
        // 简单的校验和计算，实际使用中可能需要更复杂的算法
        ByteBuffer buffer = ByteBuffer.wrap(headerData, 0, CHECKSUM_OFFSET);
        int sum = 0;
        while (buffer.hasRemaining()) {
            sum += buffer.get() & 0xFF;
        }
        return sum;
    }

    // 获取整个头部数据
    public byte[] getData() {
        return headerData;
    }
}
package org.lupenghan.eazydb.backend.DataManager.PageManager.Impl;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageHeader;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * 页面的具体实现类
 */
public class PageImpl implements Page {
    // 页面大小，例如4KB
    public static final int PAGE_SIZE = 4096;

    private PageID pageID;
    private byte[] data;
    private boolean isDirty;
    private int pinCount;
    private long lsn; // 日志序列号

    /**
     * 创建新页面
     * @param pageID 页面ID
     */
    public PageImpl(PageID pageID) {
        this.pageID = pageID;
        this.data = new byte[PAGE_SIZE];
        this.isDirty = false;
        this.pinCount = 0;
        this.lsn = 0;

        // 初始化页面头部
        PageHeader header = new PageHeader();
        header.setPageID(pageID);
        header.setLSN(0);
        header.setFreeSpacePointer((short)(PageHeader.PAGE_HEADER_SIZE));
        header.setRecordCount((short)0);
        header.setChecksum(header.calculateChecksum());

        // 将头部数据复制到页面数据中
        System.arraycopy(header.getData(), 0, data, 0, PageHeader.PAGE_HEADER_SIZE);
    }

    /**
     * 从现有数据创建页面
     * @param pageID 页面ID
     * @param data 页面数据
     */
    public PageImpl(PageID pageID, byte[] data) {
        if (data.length != PAGE_SIZE) {
            throw new IllegalArgumentException("页面数据大小必须为" + PAGE_SIZE + "字节");
        }
        this.pageID = pageID;
        this.data = data;
        this.isDirty = false;
        this.pinCount = 0;

        // 从数据中解析LSN
        ByteBuffer buffer = ByteBuffer.wrap(data, 8, 8);
        this.lsn = buffer.getLong();
    }

    @Override
    public PageID getPageID() {
        return pageID;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public long getLSN() {
        return lsn;
    }

    @Override
    public void setLSN(long lsn) {
        this.lsn = lsn;
        // 更新页面数据中的LSN
        ByteBuffer buffer = ByteBuffer.wrap(data, 8, 8);
        buffer.putLong(lsn);
    }

    @Override
    public boolean isDirty() {
        return isDirty;
    }

    @Override
    public void setDirty(boolean isDirty) {
        this.isDirty = isDirty;
    }

    @Override
    public void incrementPinCount() {
        pinCount++;
    }

    @Override
    public void decrementPinCount() {
        if (pinCount > 0) {
            pinCount--;
        }
    }

    @Override
    public int getPinCount() {
        return pinCount;
    }

    @Override
    public void writeData(int offset, byte[] data) {
        if (offset + data.length > PAGE_SIZE) {
            throw new IllegalArgumentException("写入操作超出页面边界");
        }
        System.arraycopy(data, 0, this.data, offset, data.length);
        setDirty(true);
    }

    @Override
    public byte[] readData(int offset, int length) {
        if (offset + length > PAGE_SIZE) {
            throw new IllegalArgumentException("读取操作超出页面边界");
        }
        byte[] result = new byte[length];
        System.arraycopy(this.data, offset, result, 0, length);
        return result;
    }

    /**
     * 获取页面头部
     * @return 页面头部对象
     */
    public PageHeader getHeader() {
        byte[] headerData = new byte[PageHeader.PAGE_HEADER_SIZE];
        System.arraycopy(data, 0, headerData, 0, PageHeader.PAGE_HEADER_SIZE);
        return new PageHeader(headerData);
    }

    /**
     * 更新页面头部
     * @param header 新的页面头部
     */
    public void updateHeader(PageHeader header) {
        System.arraycopy(header.getData(), 0, data, 0, PageHeader.PAGE_HEADER_SIZE);
        setDirty(true);
    }

    /**
     * 校验页面数据完整性
     * @return 如果校验和匹配则返回true，否则返回false
     */
    public boolean verifyChecksum() {
        PageHeader header = getHeader();
        int storedChecksum = header.getChecksum();
        int calculatedChecksum = header.calculateChecksum();
        return storedChecksum == calculatedChecksum;
    }
}
package org.lupenghan.eazydb.page.interfaces;

import org.lupenghan.eazydb.page.models.Page;

import java.io.IOException;

public interface PageManager {
    Page createPage();
    Page readPage(int pageId) throws IOException;

    void writePage(Page page) throws IOException;

    //获得总页数
    int getTotalPages();


    /**
     * 设置页面类型
     * @param page 页面
     * @param type 页面类型
     */
    void setPageType(Page page, byte type);

    /**
     * 获取页面空闲空间
     * @param page 页面
     * @return 空闲空间大小
     */
    int getFreeSpace(Page page);

    /**
     * 压缩页面
     * @param page 要压缩的页面
     */
    void compactPage(Page page);

    /**
     * 检查页面是否需要压缩
     * @param page 要检查的页面
     * @return 是否需要压缩
     */
    boolean needsCompaction(Page page);


}

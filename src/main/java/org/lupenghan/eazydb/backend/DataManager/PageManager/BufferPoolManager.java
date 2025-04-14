package org.lupenghan.eazydb.backend.DataManager.PageManager;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;

/**
 * 缓冲池管理接口 - 管理内存中的页面缓存
 */
public interface BufferPoolManager {
    /**
     * 从缓冲池获取指定的页面，如果不在缓冲池中则从磁盘加载
     * @param pageID 页面ID
     * @return 获取的页面，如果无法获取则返回null
     */
    Page fetchPage(PageID pageID);

    /**
     * 创建新页面
     * @return 新创建的页面，如果无法创建则返回null
     */
    Page newPage();

    /**
     * 从缓冲池中删除页面
     * @param pageID 要删除的页面ID
     * @return 操作是否成功
     */
    boolean deletePage(PageID pageID);

    /**
     * 将页面固定在缓冲池中，防止被替换出去
     * @param pageID 要固定的页面ID
     * @return 固定的页面，如果无法固定则返回null
     */
    Page pinPage(PageID pageID);

    /**
     * 解除页面的固定状态
     * @param pageID 要解除固定的页面ID
     * @param isDirty 是否标记为脏页
     * @return 操作是否成功
     */
    boolean unpinPage(PageID pageID, boolean isDirty);

    /**
     * 将所有脏页刷新到磁盘
     * @return 操作是否成功
     */
    boolean flushAllPages();

    /**
     * 将指定页面刷新到磁盘
     * @param pageID 要刷新的页面ID
     * @return 操作是否成功
     */
    boolean flushPage(PageID pageID);

    /**
     * 获取当前缓冲池中的页面数量
     * @return 页面数量
     */
    int getPoolSize();

    /**
     * 获取可用的缓冲池页框数量
     * @return 可用页框数量
     */
    int getAvailableFrames();
}
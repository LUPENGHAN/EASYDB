package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;

import java.util.List;

public interface TablespaceManager {
    // 为表分配新页面
    PageID allocatePage(int tableID) throws Exception;

    // 释放表的页面
    void freePage(PageID pageID, int tableID) throws Exception;

    // 获取表的所有页面
    List<PageID> getTablePages(int tableID) throws Exception;

    // 获取具有足够空间的页面（用于插入新记录）
    PageID getPageWithSpace(int tableID, int requiredSpace) throws Exception;
}
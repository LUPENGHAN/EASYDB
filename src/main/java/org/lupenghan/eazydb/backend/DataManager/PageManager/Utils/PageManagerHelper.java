package org.lupenghan.eazydb.backend.DataManager.PageManager.Utils;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Page;
import org.lupenghan.eazydb.backend.DataManager.PageManager.PageManager;

/**
 * 页面管理器辅助工具类
 */
public class PageManagerHelper {

    /**
     * 安全获取页面，处理异常情况
     *
     * @param pageManager 页面管理器
     * @param pageID 页面ID
     * @return 页面对象，如果获取失败则返回null
     */
    public static Page safeGetPage(PageManager pageManager, PageID pageID) {
        try {
            return pageManager.pinPage(pageID);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 安全释放页面，处理异常情况
     *
     * @param pageManager 页面管理器
     * @param pageID 页面ID
     * @param isDirty 是否为脏页
     */
    public static void safeReleasePage(PageManager pageManager, PageID pageID, boolean isDirty) {
        try {
            pageManager.unpinPage(pageID, isDirty);
        } catch (Exception e) {
            // 忽略异常
        }
    }

    /**
     * 创建新页面，处理异常情况
     *
     * @param pageManager 页面管理器
     * @return 新页面ID，如果创建失败则返回null
     */
    public static PageID safeCreatePage(PageManager pageManager) {
        try {
            return pageManager.allocatePage();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 安全删除页面，处理异常情况
     *
     * @param pageManager 页面管理器
     * @param pageID 页面ID
     * @return 操作是否成功
     */
    public static boolean safeDeletePage(PageManager pageManager, PageID pageID) {
        try {
            return pageManager.freePage(pageID);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 检查页面是否存在且可访问
     *
     * @param pageManager 页面管理器
     * @param pageID 页面ID
     * @return 如果页面存在且可访问，则返回true
     */
    public static boolean pageExists(PageManager pageManager, PageID pageID) {
        Page page = safeGetPage(pageManager, pageID);
        if (page != null) {
            safeReleasePage(pageManager, pageID, false);
            return true;
        }
        return false;
    }
}
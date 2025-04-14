package org.lupenghan.eazydb.backend.DataManager.PageManager;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;

/**
 * 页面接口 - 定义页面的基本结构和操作
 */
public interface Page {
    /**
     * 获取页面ID
     * @return 页面ID
     */
    PageID getPageID();

    /**
     * 获取页面数据
     * @return 页面数据字节数组
     */
    byte[] getData();

    /**
     * 获取页面的LSN(日志序列号)
     * @return 页面的LSN
     */
    long getLSN();

    /**
     * 设置页面的LSN
     * @param lsn 新的LSN值
     */
    void setLSN(long lsn);

    /**
     * 判断页面是否为脏页
     * @return 如果页面被修改过，返回true；否则返回false
     */
    boolean isDirty();

    /**
     * 标记页面为脏页
     * @param isDirty 是否为脏页
     */
    void setDirty(boolean isDirty);

    /**
     * 增加页面的引用计数
     */
    void incrementPinCount();

    /**
     * 减少页面的引用计数
     */
    void decrementPinCount();

    /**
     * 获取页面的引用计数
     * @return 当前引用计数
     */
    int getPinCount();

    /**
     * 在指定位置写入数据
     * @param offset 起始偏移量
     * @param data 要写入的数据
     */
    void writeData(int offset, byte[] data);

    /**
     * 从指定位置读取数据
     * @param offset 起始偏移量
     * @param length 要读取的字节数
     * @return 读取的数据
     */
    byte[] readData(int offset, int length);
}
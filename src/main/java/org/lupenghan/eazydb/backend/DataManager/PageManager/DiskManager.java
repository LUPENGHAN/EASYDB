package org.lupenghan.eazydb.backend.DataManager.PageManager;

import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;

import java.io.IOException;

/**
 * 磁盘管理器接口 - 负责页面的物理存储和读写
 */
public interface DiskManager {
    /**
     * 从磁盘读取页面
     * @param pageID 页面ID
     * @return 页面数据，如果页面不存在则返回null
     * @throws IOException 读取过程中发生IO错误
     */
    byte[] readPage(PageID pageID) throws IOException;

    /**
     * 将页面写入磁盘
     * @param pageID 页面ID
     * @param data 页面数据
     * @throws IOException 写入过程中发生IO错误
     */
    void writePage(PageID pageID, byte[] data) throws IOException;

    /**
     * 分配新的页面
     * @return 新页面的ID
     * @throws IOException 分配过程中发生IO错误
     */
    PageID allocatePage() throws IOException;

    /**
     * 释放页面
     * @param pageID 要释放的页面ID
     * @return 操作是否成功
     * @throws IOException 释放过程中发生IO错误
     */
    boolean deallocatePage(PageID pageID) throws IOException;

    /**
     * 获取数据库文件的页数
     * @param fileID 文件ID
     * @return 页数
     * @throws IOException 获取过程中发生IO错误
     */
    int getFilePageCount(int fileID) throws IOException;

    /**
     * 创建新的数据库文件
     * @param fileName 文件名
     * @return 分配的文件ID
     * @throws IOException 创建过程中发生IO错误
     */
    int createFile(String fileName) throws IOException;

    /**
     * 打开现有的数据库文件
     * @param fileName 文件名
     * @return 文件ID
     * @throws IOException 打开过程中发生IO错误
     */
    int openFile(String fileName) throws IOException;

    /**
     * 关闭数据库文件
     * @param fileID 文件ID
     * @throws IOException 关闭过程中发生IO错误
     */
    void closeFile(int fileID) throws IOException;

    /**
     * 删除数据库文件
     * @param fileName 文件名
     * @return 操作是否成功
     * @throws IOException 删除过程中发生IO错误
     */
    boolean deleteFile(String fileName) throws IOException;

    /**
     * 刷新所有待写入数据到磁盘
     * @throws IOException 刷新过程中发生IO错误
     */
    void flushAll() throws IOException;

    /**
     * 关闭磁盘管理器
     * @throws IOException 关闭过程中发生IO错误
     */
    void close() throws IOException;
}
package org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform;

import lombok.Getter;

/**
 * 页面标识符类，用于唯一标识数据库中的一个页面
 */
@Getter
public class PageID {

    private final int fileID;    // 文件ID，用于标识页面所在的文件

    private final int pageNum;   // 页面编号，标识文件中的页面序号

    /**
     * 创建页面标识符
     * @param fileID 文件ID
     * @param pageNum 页面编号
     */
    public PageID(int fileID, int pageNum) {
        this.fileID = fileID;
        this.pageNum = pageNum;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PageID other = (PageID) obj;
        return fileID == other.fileID && pageNum == other.pageNum;
    }

    @Override
    public int hashCode() {
        return 31 * fileID + pageNum;
    }

    @Override
    public String toString() {
        return "PageID{fileID=" + fileID + ", pageNum=" + pageNum + "}";
    }
}
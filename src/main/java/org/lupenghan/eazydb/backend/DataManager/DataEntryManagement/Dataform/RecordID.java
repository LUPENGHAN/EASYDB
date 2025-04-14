package org.lupenghan.eazydb.backend.DataManager.DataEntryManagement.Dataform;

import lombok.Getter;
import org.lupenghan.eazydb.backend.DataManager.PageManager.Dataform.PageID;

/**
 * 记录ID类，唯一标识数据库中的一条记录
 */
@Getter
public class RecordID {

    private PageID pageID;  // 页面ID

    private int slotNum;    // 槽号

    /**
     * 创建记录ID
     * @param pageID 页面ID
     * @param slotNum 槽号
     */
    public RecordID(PageID pageID, int slotNum) {
        this.pageID = pageID;
        this.slotNum = slotNum;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RecordID other = (RecordID) obj;
        return slotNum == other.slotNum && pageID.equals(other.pageID);
    }

    @Override
    public int hashCode() {
        return 31 * pageID.hashCode() + slotNum;
    }

    @Override
    public String toString() {
        return "RecordID{pageID=" + pageID + ", slotNum=" + slotNum + "}";
    }
}
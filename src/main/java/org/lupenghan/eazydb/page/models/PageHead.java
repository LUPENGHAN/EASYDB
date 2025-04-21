package org.lupenghan.eazydb.page.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PageHead {
    int pageId;                  // 4页唯一编号
    long fileOffset;            // 8在文件中的偏移（物理地址）
    long pageLSN;               // 8日志序列号，用于恢复
    byte pageType;              // 1页类型：数据页/目录页/undo页
    short freeSpacePointer;       // 2 空闲空间指针
    int slotCount;              // 4 当前 slot 数量
    int recordCount;           // 4 有效记录数量
    int checksum;               // 4 页校验和
    long version;               // 8 页面版本号（用于MVCC）
    long createTime;            // 8 页面创建时间
    long lastModifiedTime;      //8  最后修改时间
    // B+树索引相关字段
    boolean isLeaf;             // 1 是否为叶子节点
    int keyCount;              // 4 键的数量


}

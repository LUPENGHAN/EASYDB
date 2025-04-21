package org.lupenghan.eazydb.page.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PageHead {
    int pageId;                  // 页唯一编号
    long fileOffset;            // 在文件中的偏移（物理地址）
    long pageLSN;               // 日志序列号，用于恢复
    byte pageType;              // 页类型：数据页/目录页/undo页
    int prevPageId;            // 上一页页号（可选）
    int nextPageId;            // 下一页页号（可选）
    int freeSpaceDirCount;      // 空闲段数量
    int freeSpacePointer;       // 空闲空间指针
    int slotCount;              // 当前 slot 数量
    int recordCount;           // 有效记录数量
    int checksum;               // 页校验和
    long version;               // 页面版本号（用于MVCC）
    long createTime;            // 页面创建时间
    long lastModifiedTime;      // 最后修改时间
    // B+树索引相关字段
    boolean isLeaf;             // 是否为叶子节点
    int keyCount;              // 键的数量


}

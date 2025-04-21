package org.lupenghan.eazydb.page.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SlotDirectoryEntry {
    short offset;               // 记录偏移
    boolean inUse;              // 是否有效（可用于 slot reuse）
    long recordVersion;         // 记录版本号（用于MVCC）
    int pageId;                 // 记录所在页面ID
    int slotId;                 // 记录所在槽位ID
}


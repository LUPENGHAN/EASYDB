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
    int offset;               // 记录偏移 4B
    boolean inUse;              // 是否有效（可用于 slot reuse） 1B
    byte reserved1;          // 1B
    short reserved2;         // 1B

}


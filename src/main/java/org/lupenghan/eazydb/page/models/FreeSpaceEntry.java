package org.lupenghan.eazydb.page.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public  class FreeSpaceEntry {
    int offset;     // 空间起始地址
    int length;     // 空间长度
}
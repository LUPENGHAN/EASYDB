package org.lupenghan.eazydb.page.models;

import lombok.Getter;
import lombok.Setter;

@Getter
public enum PageType {
    DATA((byte)0),             // 数据页
    INDEX((byte)1),            // 索引页
    UNDO((byte)2);             // UNDO页

    private final byte value;
    PageType(byte value) {
        this.value = value;
    }
}

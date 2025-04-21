package org.lupenghan.eazydb.record.models;

import lombok.Getter;

@Getter
public enum RecordStatus {
    ACTIVE ((byte)0)  ,
    DELETED ((byte)1),
    UPDATED ((byte)2) ;
    private final byte value;
    RecordStatus(byte value) {
        this.value = value;
    }
}

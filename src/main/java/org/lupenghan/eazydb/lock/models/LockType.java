package org.lupenghan.eazydb.lock.models;

import lombok.Getter;

@Getter
public enum LockType {
    //共享锁
    SHARED_LOCK(0),
    // 排他锁
    EXCLUSIVE_LOCK(1);

    private final int value;
    LockType(int value) {
        this.value = value;
    }

    public static LockType fromValue(int value) {
        for (LockType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid lock type value: " + value);
    }
}

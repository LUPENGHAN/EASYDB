package org.lupenghan.eazydb.transaction.models;

public enum TransactionStatus {
    ACTIVE,
    COMMITTED,
//  用于用户主动回滚
//   ROLLED_BACK,
    ABORTED
}

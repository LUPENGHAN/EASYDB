package org.lupenghan.eazydb.backend.DataManager.LogManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecoveryManager {
    private static class TransactionInfo {
        boolean committed;
        List<Long> redoLSNs;
        List<Long> undoLSNs;

        public TransactionInfo() {
            this.committed = false;
            this.redoLSNs = new ArrayList<>();
            this.undoLSNs = new ArrayList<>();
        }
    }

    private LogManager logManager;
    private Map<Long, TransactionInfo> activeTransactions; // XID -> TransactionInfo

    public RecoveryManager(LogManager logManager) {
        this.logManager = logManager;
        this.activeTransactions = new HashMap<>();
    }

    // 执行恢复过程
    public void recover() {
        // 1. 分析阶段
        analyzePhase();

        // 2. 重做阶段
        redoPhase();

        // 3. 撤销阶段
        undoPhase();

        // 4. 创建新检查点
        logManager.checkpoint();
    }

    private void analyzePhase() {
        // 从最近的检查点开始扫描日志
        LogManager.LogIterator iterator = logManager.iterator();

        while(iterator.hasNext()) {
            byte[] logData = iterator.next();
            long lsn = iterator.position() - logData.length;

            if(iterator.isRedo()) {
                // 处理REDO日志
                // 解析事务ID
                // 记录该事务的redo日志
            } else {
                // 处理UNDO日志或其他类型日志
                // 解析事务ID
                // 如果是提交日志，标记事务为已提交
                // 如果是回滚日志，标记事务为已回滚
                // 否则，记录该事务的undo日志
            }
        }
    }

    private void redoPhase() {
        // 重做所有日志操作(不管事务是否已提交)
        LogManager.LogIterator iterator = logManager.iterator();

        while(iterator.hasNext()) {
            byte[] logData = iterator.next();

            if(iterator.isRedo()) {
                // 重做该操作
                // 这里需要调用PageManager等组件进行实际的数据修改
            }
        }
    }

    private void undoPhase() {
        // 撤销所有未提交事务的操作
        for(Map.Entry<Long, TransactionInfo> entry : activeTransactions.entrySet()) {
            if(!entry.getValue().committed) {
                // 事务未提交，需要撤销
                List<Long> undoLSNs = entry.getValue().undoLSNs;

                // 注意：撤销应该按照相反的顺序进行
                for(int i = undoLSNs.size() - 1; i >= 0; i--) {
                    long lsn = undoLSNs.get(i);
                    byte[] logData = logManager.readUndoLog(lsn);

                    // 执行撤销操作
                    // 这里需要调用相应的数据管理组件进行操作
                }
            }
        }
    }
}
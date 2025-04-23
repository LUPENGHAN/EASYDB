package org.lupenghan;

import org.lupenghan.eazydb.lock.Impl.LockManagerImpl;
import org.lupenghan.eazydb.log.Impl.LogManagerImpl;
import org.lupenghan.eazydb.page.Impl.PageManagerImpl;
import org.lupenghan.eazydb.record.Impl.RecordManagerImpl;
import org.lupenghan.eazydb.table.Impl.TableManagerImpl;
import org.lupenghan.eazydb.table.models.Table;
import org.lupenghan.eazydb.transaction.Impl.TransactionManagerImpl;
import org.lupenghan.parser.SQLParser;
import org.lupenghan.parser.SQLParser.Command;
import org.lupenghan.parser.SQLParser.CommandType;
import org.lupenghan.parser.TableParser;
import org.lupenghan.query.Impl.QueryEngineImpl;
import org.lupenghan.query.interfaces.QueryEngine;
import org.lupenghan.eazydb.table.interfaces.TableManager;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class MainCLI {

    private final QueryEngine queryEngine;

    public MainCLI(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("欢迎使用 EazyDB 🌱 简易数据库。输入 SQL 或 exit 退出。");

        while (true) {
            System.out.print("\n> ");
            String sql = scanner.nextLine().trim();
            if (sql.equalsIgnoreCase("exit") || sql.equalsIgnoreCase("quit")) break;
            if (sql.isEmpty()) continue;

            try {
                Command cmd = SQLParser.parse(sql);

                switch (cmd.getType()) {
                    case CREATE_TABLE -> {
                        Table table = TableParser.parseCreateTable(cmd.getRaw());
                        queryEngine.createTable(table);
                        System.out.println("✅ 创建表成功: " + table.getName());
                    }

                    case DROP_TABLE -> {
                        boolean dropped = queryEngine.dropTable(cmd.getTableName());
                        if (dropped) System.out.println("🗑️ 表已删除: " + cmd.getTableName());
                        else System.out.println("⚠️ 表不存在: " + cmd.getTableName());
                    }

                    case INSERT -> {
                        // 获取表定义，用于处理列和数据
                        Table table = ((QueryEngineImpl)queryEngine).getTableManager().getTable(cmd.getTableName());
                        if (table == null) {
                            System.out.println("⚠️ 表不存在: " + cmd.getTableName());
                            break;
                        }
                        
                        // 处理值数据
                        String valueStr = new String(cmd.getValue());
                        System.out.println("插入数据: " + valueStr);
                        
                        // 开始事务并执行插入
                        long xid = queryEngine.beginTransaction();
                        try {
                            queryEngine.insert(xid, cmd.getTableName(), cmd.getValue());
                            queryEngine.commitTransaction(xid);
                            System.out.println("✅ 插入成功");
                        } catch (Exception e) {
                            try {
                                queryEngine.rollbackTransaction(xid);
                            } catch (Exception ex) {
                                System.err.println("回滚失败: " + ex.getMessage());
                            }
                            throw e;
                        }
                    }

                    case SELECT_ALL -> {
                        List<byte[]> rows = queryEngine.selectAll(cmd.getTableName());
                        System.out.println("📄 查询结果：");
                        for (byte[] row : rows) {
                            System.out.println(" - " + new String(row));
                        }
                    }

                    case SELECT_ONE -> {
                        byte[] data = queryEngine.select(cmd.getTableName(), cmd.getPageId(), cmd.getSlotId());
                        if (data != null) {
                            System.out.println("📍 查询结果: " + new String(data));
                        } else {
                            System.out.println("⚠️ 没有找到记录");
                        }
                    }

                    case DELETE -> {
                        long xid = queryEngine.beginTransaction();
                        queryEngine.delete(xid, cmd.getTableName(), cmd.getPageId(), cmd.getSlotId());
                        queryEngine.commitTransaction(xid);
                        System.out.println("🗑️ 删除成功");
                    }

                    case UPDATE -> {
                        long xid = queryEngine.beginTransaction();
                        queryEngine.update(xid, cmd.getTableName(), cmd.getPageId(), cmd.getSlotId(), cmd.getValue());
                        queryEngine.commitTransaction(xid);
                        System.out.println("✏️ 更新成功");
                    }

                    case UNKNOWN -> {
                        System.out.println("❌ 无法解析的命令，请检查 SQL 语法");
                    }
                }

            } catch (Exception e) {
                System.err.println("⚠️ 执行出错：" + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("👋 再见！");
    }
    public static void main(String[] args) throws Exception {
        // 初始化组件
        var pageManager = new PageManagerImpl("data/page/page.page");
        var logManager = new LogManagerImpl();
        var lockManager = new LockManagerImpl();
        var transactionManager = new TransactionManagerImpl(logManager, lockManager,pageManager);
        var recordManager = new RecordManagerImpl(pageManager, logManager, transactionManager);
        var tableManager = new TableManagerImpl();

        // 创建引擎
        QueryEngine queryEngine = new QueryEngineImpl(
                tableManager,
                pageManager,
                recordManager,
                transactionManager
        );

        // 启动 CLI
        MainCLI cli = new MainCLI(queryEngine);
        cli.run();
    }

}

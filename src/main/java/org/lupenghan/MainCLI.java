package org.lupenghan;

import org.lupenghan.eazydb.lock.interfaces.LockManager;
import org.lupenghan.eazydb.log.interfaces.LogManager;
import org.lupenghan.eazydb.page.interfaces.PageManager;
import org.lupenghan.eazydb.record.interfaces.RecordManager;
import org.lupenghan.query.Impl.QueryEngineImpl;
import org.lupenghan.query.interfaces.QueryEngine;
import org.lupenghan.parser.SQLParser;
import org.lupenghan.parser.SQLParser.Command;
import org.lupenghan.parser.SQLParser.CommandType;

// 以下这些 manager 要你自己传入或初始化
import org.lupenghan.eazydb.page.Impl.PageManagerImpl;
import org.lupenghan.eazydb.record.Impl.RecordManagerImpl;
import org.lupenghan.eazydb.log.Impl.LogManagerImpl;
import org.lupenghan.eazydb.lock.Impl.LockManagerImpl;
import org.lupenghan.eazydb.table.Impl.TableManagerImpl;
import org.lupenghan.eazydb.transaction.Impl.TransactionManagerImpl;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class MainCLI {

    public static void main(String[] args) throws IOException {
        var tableManager = new TableManagerImpl();
        var pageManager = new PageManagerImpl("data/pages.db");
        var logManager = new LogManagerImpl("data/wal.log");
        var lockManager = new LockManagerImpl();
        var transactionManager = new TransactionManagerImpl(logManager, lockManager,pageManager);
        var recordManager = new RecordManagerImpl(pageManager, logManager, transactionManager);

        QueryEngine queryEngine = new QueryEngineImpl(
                tableManager, pageManager, recordManager, transactionManager
        );

        Scanner scanner = new Scanner(System.in);
        System.out.println("✅ 欢迎使用 EazyDB 交互终端，输入 SQL 或 exit");

        long xid = queryEngine.beginTransaction();

        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();
            if (input.equalsIgnoreCase("exit")) break;

            try {
                Command command = SQLParser.parse(input);

                switch (command.getType()) {
                    case CREATE_TABLE -> {
                        tableManager.createTable(TableParser.parse(command.getRaw()));
                        System.out.println("✅ 表创建成功");
                    }
                    case DROP_TABLE -> {
                        boolean dropped = tableManager.dropTable(command.getTableName());
                        System.out.println(dropped ? "✅ 表删除成功" : "❌ 删除失败");
                    }
                    case INSERT -> {
                        queryEngine.insert(xid, command.getTableName(), command.getValue());
                        System.out.println("✅ 插入成功");
                    }
                    case SELECT_ONE -> {
                        byte[] result = queryEngine.select(command.getTableName(), command.getPageId(), command.getSlotId());
                        System.out.println(result == null ? "❌ 未找到记录" : "🎯 记录内容: " + new String(result));
                    }
                    case SELECT_ALL -> {
                        List<byte[]> records = queryEngine.selectAll(command.getTableName());
                        System.out.println("📋 共查询到 " + records.size() + " 条记录:");
                        for (byte[] rec : records) {
                            System.out.println(" - " + new String(rec));
                        }
                    }
                    case UPDATE -> {
                        queryEngine.update(xid, command.getTableName(), command.getPageId(), command.getSlotId(), command.getValue());
                        System.out.println("✅ 更新成功");
                    }
                    case DELETE -> {
                        queryEngine.delete(xid, command.getTableName(), command.getPageId(), command.getSlotId());
                        System.out.println("✅ 删除成功");
                    }
                    default -> System.out.println("❓ 无效命令或暂不支持");
                }

            } catch (Exception e) {
                System.err.println("💥 执行失败: " + e.getMessage());
            }
        }

        queryEngine.commitTransaction(xid);
        System.out.println("🧾 事务已提交，Bye!");
    }
}

package org.lupenghan.parser;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

public class SQLParser {

    public static Command parse(String sql) {
        sql = sql.trim().replaceAll(";$", "");

        if (sql.toUpperCase().startsWith("CREATE TABLE")) {
            return Command.builder()
                    .type(CommandType.CREATE_TABLE)
                    .raw(sql)
                    .build();
        }

        if (sql.toUpperCase().startsWith("DROP TABLE")) {
            String[] parts = sql.split("\\s+");
            return Command.builder()
                    .type(CommandType.DROP_TABLE)
                    .tableName(parts[2])
                    .build();
        }

        if (sql.toUpperCase().startsWith("SELECT * FROM")) {
            String[] parts = sql.split("\\s+");
            return Command.builder()
                    .type(CommandType.SELECT_ALL)
                    .tableName(parts[3])
                    .build();
        }

        if (sql.toUpperCase().startsWith("SELECT FROM")) {
            Matcher matcher = Pattern.compile("SELECT FROM (\\w+) WHERE page=(\\d+) AND slot=(\\d+)", Pattern.CASE_INSENSITIVE).matcher(sql);
            if (matcher.find()) {
                return Command.builder()
                        .type(CommandType.SELECT_ONE)
                        .tableName(matcher.group(1))
                        .pageId(Integer.parseInt(matcher.group(2)))
                        .slotId(Integer.parseInt(matcher.group(3)))
                        .build();
            }
        }

        if (sql.toUpperCase().startsWith("DELETE FROM")) {
            Matcher matcher = Pattern.compile("DELETE FROM (\\w+) WHERE page=(\\d+) AND slot=(\\d+)", Pattern.CASE_INSENSITIVE).matcher(sql);
            if (matcher.find()) {
                return Command.builder()
                        .type(CommandType.DELETE)
                        .tableName(matcher.group(1))
                        .pageId(Integer.parseInt(matcher.group(2)))
                        .slotId(Integer.parseInt(matcher.group(3)))
                        .build();
            }
        }

        if (sql.toUpperCase().startsWith("UPDATE")) {
            Matcher matcher = Pattern.compile("UPDATE (\\w+) SET value='(.+)' WHERE page=(\\d+) AND slot=(\\d+)", Pattern.CASE_INSENSITIVE).matcher(sql);
            if (matcher.find()) {
                return Command.builder()
                        .type(CommandType.UPDATE)
                        .tableName(matcher.group(1))
                        .value(matcher.group(2).getBytes())
                        .pageId(Integer.parseInt(matcher.group(3)))
                        .slotId(Integer.parseInt(matcher.group(4)))
                        .build();
            }
        }

        if (sql.toUpperCase().startsWith("INSERT INTO")) {
            Matcher matcher = Pattern.compile("INSERT INTO (\\w+) VALUES \\((.+)\\)", Pattern.CASE_INSENSITIVE).matcher(sql);
            if (matcher.find()) {
                String tableName = matcher.group(1);
                String values = matcher.group(2);
                return Command.builder()
                        .type(CommandType.INSERT)
                        .tableName(tableName)
                        .value(values.getBytes()) // 简化处理：实际中应根据字段类型解析
                        .build();
            }
        }

        return Command.builder().type(CommandType.UNKNOWN).build();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Command {
        private CommandType type;
        private String raw;
        private String tableName;
        private int pageId;
        private int slotId;
        private byte[] value;
    }

    public enum CommandType {
        CREATE_TABLE,
        DROP_TABLE,
        INSERT,
        SELECT_ALL,
        SELECT_ONE,
        UPDATE,
        DELETE,
        UNKNOWN
    }
}

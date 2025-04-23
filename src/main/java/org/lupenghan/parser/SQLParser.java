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
        // 去除结尾分号并清理空白
        sql = sql.trim().replaceAll(";$", "");

        // 获取SQL命令的第一个词，用于初步识别命令类型
        String firstWord = sql.split("\\s+")[0].toUpperCase();

        switch (firstWord) {
            case "CREATE":
                if (sql.toUpperCase().startsWith("CREATE TABLE")) {
                    return Command.builder()
                            .type(CommandType.CREATE_TABLE)
                            .raw(sql)
                            .build();
                }
                break;

            case "DROP":
                if (sql.toUpperCase().startsWith("DROP TABLE")) {
                    String[] parts = sql.split("\\s+", 3);
                    if (parts.length >= 3) {
                        return Command.builder()
                                .type(CommandType.DROP_TABLE)
                                .tableName(parts[2])
                                .build();
                    }
                }
                break;

            case "SELECT":
                if (sql.toUpperCase().startsWith("SELECT * FROM")) {
                    String[] parts = sql.split("\\s+", 4);
                    if (parts.length >= 4) {
                        return Command.builder()
                                .type(CommandType.SELECT_ALL)
                                .tableName(parts[3])
                                .build();
                    }
                } else if (sql.toUpperCase().startsWith("SELECT FROM")) {
                    Pattern pattern = Pattern.compile("SELECT FROM (\\w+) WHERE page=(\\d+) AND slot=(\\d+)",
                            Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(sql);
                    if (matcher.find()) {
                        return Command.builder()
                                .type(CommandType.SELECT_ONE)
                                .tableName(matcher.group(1))
                                .pageId(Integer.parseInt(matcher.group(2)))
                                .slotId(Integer.parseInt(matcher.group(3)))
                                .build();
                    }
                }
                break;

            case "DELETE":
                if (sql.toUpperCase().startsWith("DELETE FROM")) {
                    Pattern pattern = Pattern.compile("DELETE FROM (\\w+) WHERE page=(\\d+) AND slot=(\\d+)",
                            Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(sql);
                    if (matcher.find()) {
                        return Command.builder()
                                .type(CommandType.DELETE)
                                .tableName(matcher.group(1))
                                .pageId(Integer.parseInt(matcher.group(2)))
                                .slotId(Integer.parseInt(matcher.group(3)))
                                .build();
                    }
                }
                break;

            case "UPDATE":
                // 优化UPDATE命令解析，支持更灵活的语法
                // 支持 UPDATE table SET value='value' WHERE page=N AND slot=M
                // 或 UPDATE table SET col='value' WHERE page=N AND slot=M
                Pattern updatePattern = Pattern.compile(
                        "UPDATE (\\w+) SET (?:value|\\w+)=['\"]?([^'\"]+)['\"]? WHERE page=(\\d+) AND slot=(\\d+)",
                        Pattern.CASE_INSENSITIVE);
                Matcher updateMatcher = updatePattern.matcher(sql);
                if (updateMatcher.find()) {
                    return Command.builder()
                            .type(CommandType.UPDATE)
                            .tableName(updateMatcher.group(1))
                            .value(updateMatcher.group(2).getBytes())
                            .pageId(Integer.parseInt(updateMatcher.group(3)))
                            .slotId(Integer.parseInt(updateMatcher.group(4)))
                            .build();
                }
                break;

            case "INSERT":
                if (sql.toUpperCase().startsWith("INSERT INTO")) {
                    Pattern pattern = Pattern.compile(
                            "INSERT INTO (\\w+)(?:\\s*\\(([^)]+)\\))?\\s+VALUES\\s+\\((.+)\\)",
                            Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(sql);
                    if (matcher.find()) {
                        String tableName = matcher.group(1);
                        String columnsPart = matcher.group(2);
                        String valuesPart = matcher.group(3);

                        String[] columns = null;
                        if (columnsPart != null && !columnsPart.trim().isEmpty()) {
                            columns = columnsPart.split("\\s*,\\s*");
                        }

                        // 清理引号并保留值
                        String cleanValue = valuesPart.replaceAll("['\"]", "").trim();

                        return Command.builder()
                                .type(CommandType.INSERT)
                                .tableName(tableName)
                                .columns(columns)
                                .value(cleanValue.getBytes())
                                .build();
                    }
                }
                break;
        }

        // 如果没有匹配到任何已知命令，返回UNKNOWN
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
        private String[] columns;
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
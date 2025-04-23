package org.lupenghan.parser;

import org.lupenghan.eazydb.table.models.*;
import org.lupenghan.eazydb.table.models.DataType;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 负责解析 CREATE TABLE 语句，生成 Table 对象
 */
public class TableParser {

    // 支持简单的 CREATE TABLE 语法解析
    public static Table parseCreateTable(String sql) {
        sql = sql.trim().replaceAll(";", "");

        Pattern pattern = Pattern.compile(
                "CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.*)\\)", Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(sql);
        if (!matcher.find()) {
            throw new IllegalArgumentException("无效的 CREATE TABLE 语句");
        }

        String tableName = matcher.group(1);
        String body = matcher.group(2);

        List<Column> columns = new ArrayList<>();
        List<String> uniqueKeys = new ArrayList<>();
        List<ForeignKey> foreignKeys = new ArrayList<>();
        String primaryKey = null;

        String[] parts = body.split(",");

        for (String part : parts) {
            part = part.trim();

            // 处理 PRIMARY KEY
            if (part.toUpperCase().startsWith("PRIMARY KEY")) {
                primaryKey = extractKeyName(part);
                continue;
            }

            // 处理 UNIQUE
            if (part.toUpperCase().startsWith("UNIQUE")) {
                uniqueKeys.add(extractKeyName(part));
                continue;
            }

            // 处理 FOREIGN KEY
            if (part.toUpperCase().startsWith("FOREIGN KEY")) {
                foreignKeys.add(parseForeignKey(part));
                continue;
            }

            // 处理普通列
            Column column = parseColumn(part);
            columns.add(column);
        }

        return Table.builder()
                .name(tableName)
                .columns(columns)
                .primaryKey(primaryKey)
                .uniqueKeys(uniqueKeys)
                .foreignKeys(foreignKeys)
                .createdTime(System.currentTimeMillis())
                .lastModifiedTime(System.currentTimeMillis())
                .rowCount(0)
                .pageCount(0)
                .build();
    }

    public static String extractKeyName(String part) {
        Pattern keyPattern = Pattern.compile("\\((\\w+)\\)");
        Matcher keyMatcher = keyPattern.matcher(part);
        if (keyMatcher.find()) {
            return keyMatcher.group(1);
        }
        throw new IllegalArgumentException("未找到括号中的列名: " + part);
    }

    private static Column parseColumn(String part) {
        String[] tokens = part.trim().split("\\s+");

        if (tokens.length < 2) {
            throw new IllegalArgumentException("无效的列定义: " + part);
        }

        String name = tokens[0];
        String typeStr = tokens[1].toUpperCase();

        DataType dataType;
        int length = 0;
        if (typeStr.startsWith("VARCHAR")) {
            dataType = DataType.VARCHAR;
            length = extractLength(typeStr);
        } else if (typeStr.equals("INT")) {
            dataType = DataType.INT;
        }else if (typeStr.startsWith("BIGINT")) {
            dataType = DataType.BIGINT;
        }else if (typeStr.startsWith("FLOAT")) {
            dataType = DataType.FLOAT;
        }else if (typeStr.startsWith("DOUBLE")) {
            dataType = DataType.DOUBLE;
        }else if (typeStr.equals("DATE")) {
            dataType = DataType.DATE;
        } else if (typeStr.equals("BOOLEAN")) {
            dataType = DataType.BOOLEAN;
        } else if (typeStr.equals("TIMESTAMP")) {
            dataType = DataType.TIMESTAMP;
        } else {
            throw new IllegalArgumentException("不支持的数据类型: " + typeStr);
        }

        boolean nullable = true;
        boolean isPrimary = false;
        String defaultValue = null;

        for (int i = 2; i < tokens.length; i++) {
            String token = tokens[i].toUpperCase();
            if (token.equals("NOT")) {
                if (i + 1 < tokens.length && tokens[i + 1].equalsIgnoreCase("NULL")) {
                    nullable = false;
                    i++;
                }
            } else if (token.equals("PRIMARY")) {
                isPrimary = true;
                i++;
            } else if (token.equals("DEFAULT")) {
                if (i + 1 < tokens.length) {
                    defaultValue = tokens[i + 1].replaceAll("'", "");
                    i++;
                }
            }
        }

        return Column.builder()
                .name(name)
                .type(dataType)
                .length(length)
                .nullable(nullable)
                .isPrimary(isPrimary)
                .defaultValue(defaultValue)
                .build();
    }

    private static int extractLength(String typeStr) {
        Pattern lenPattern = Pattern.compile("VARCHAR\\((\\d+)\\)");
        Matcher matcher = lenPattern.matcher(typeStr);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new IllegalArgumentException("未找到 VARCHAR 长度定义");
    }

    private static ForeignKey parseForeignKey(String part) {
        // 示例：FOREIGN KEY (user_id) REFERENCES users(id)
        Pattern fkPattern = Pattern.compile(
                "FOREIGN KEY \\((\\w+)\\) REFERENCES (\\w+)\\((\\w+)\\)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = fkPattern.matcher(part);
        if (!matcher.find()) {
            throw new IllegalArgumentException("无效的 FOREIGN KEY 语句: " + part);
        }

        return ForeignKey.builder()
                .columnName(matcher.group(1))
                .referencedTable(matcher.group(2))
                .referencedColumn(matcher.group(3))
                .constraintName("FK_" + matcher.group(1) + "_" + matcher.group(2))
                .build();
    }
}

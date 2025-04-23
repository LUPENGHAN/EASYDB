package org.lupenghan.eazydb.table.Impl;

import lombok.Data;
import org.lupenghan.eazydb.table.interfaces.TableManager;
import org.lupenghan.eazydb.table.models.Table;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class TableManagerImpl implements TableManager {
    private static final String CATALOG_DIR = "data/catalog/";
    private final Map<String, Table> tableMap = new HashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    public TableManagerImpl() throws IOException {
        Files.createDirectories(Paths.get(CATALOG_DIR));
        loadAllSchemas();
    }
    /**
     * 启动时加载已有的 .table 文件
     */
    private void loadAllSchemas() throws IOException {
        File dir = new File(CATALOG_DIR);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".table"));
        if (files == null) return;
        for (File file : files) {
            Table table = mapper.readValue(file, Table.class);
            tableMap.put(table.getName(), table);
        }
    }
    @Override
    public void createTable(Table table) throws IOException {
        String name = table.getName();
        if (tableMap.containsKey(name)) {
            throw new IllegalArgumentException("Table already exists: " + name);
        }
        File file = new File(CATALOG_DIR + name + ".table");
        mapper.writerWithDefaultPrettyPrinter().writeValue(file, table);
        tableMap.put(name, table);
    }

    @Override
    public Table getTable(String tableName) throws IOException {
        return tableMap.get(tableName);
    }

    @Override
    public List<String> listTables() {
        return new ArrayList<>(tableMap.keySet());
    }

    @Override
    public boolean dropTable(String tableName) {
        File file = new File(CATALOG_DIR + tableName + ".table");
        if (file.exists() && file.delete()) {
            tableMap.remove(tableName);
            return true;
        }
        return false;
    }
}

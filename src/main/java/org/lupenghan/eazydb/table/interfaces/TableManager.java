package org.lupenghan.eazydb.table.interfaces;

import org.lupenghan.eazydb.table.models.Table;

import java.io.IOException;
import java.util.List;

public interface TableManager {
    void createTable(Table schema) throws IOException;
    Table getTable(String tableName) throws IOException;
    List<String> listTables();
    boolean dropTable(String tableName);

}

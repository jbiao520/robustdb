package com.robustdb.server.client;

import com.robustdb.server.model.metadata.TableDef;

import java.util.Map;

public interface KVClient {
    void createTableMetaData(TableDef tableDef);

    void createDataTable(String tableName);

    void insertData(Map<String, String> kvs, String tableName);

    byte[] getDataData(String key, String tableName);

    byte[] getMetaData(String key, String tableName);
}

package com.robustdb.server.client;

import com.robustdb.server.model.metadata.TableDef;

import java.util.List;
import java.util.Map;

public interface KVClient {
    void createTableMetaData(TableDef tableDef);

    void createDataTable(String tableName);

    void insertData(Map<String, String> kvs);

    byte[] getDataNodeData(String key);

    byte[] getMetaData(String key, String tableName);

    List<String> getSecondaryIndexesOnDataNode(String key);

    boolean containsKeyInDataNode(String key);
}

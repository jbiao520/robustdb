package com.robustdb.server.client;

import com.robustdb.server.model.metadata.TableDef;

public interface KVClient {
    void createTableMetaData(TableDef tableDef);
    void createDataTable(String tableName);
}

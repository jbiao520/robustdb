package com.robustdb.server.client;

import com.robustdb.server.model.TableDef;

public interface KVClient {
    void createTable(TableDef tableDef);
}

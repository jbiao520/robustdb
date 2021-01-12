package com.robustdb.server.handlers;

import com.robustdb.server.client.KVClient;
import com.robustdb.server.client.LocalKVClient;
import com.robustdb.server.enums.SQLType;
import com.robustdb.server.model.TableDef;

public class SQLHandler {
    KVClient kvClient = new LocalKVClient();
    public void handle(SQLType type, Object obj){
        switch (type){
            case CREATE:
                TableDef tableDef = (TableDef)obj;
                kvClient.createTable(tableDef);
                break;
            default:
                break;
        }
    }
}

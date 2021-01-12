package com.robustdb.server.client;

import com.google.gson.Gson;
import com.robustdb.kv.constants.KVConstants;
import com.robustdb.kv.rocksdb.RocksdbInstance;
import com.robustdb.server.model.TableDef;
import org.rocksdb.RocksDBException;


public class LocalKVClient implements KVClient{
    private RocksdbInstance rocksdbInstance = new RocksdbInstance();
    private static Gson gson = new Gson();

    public void createTable(TableDef tableDef) {
        String key = tableDef.getTableName();
        String jsonVal = gson.toJson(tableDef);
        try {
            rocksdbInstance.putCfKeyValue(KVConstants.META_DATA,KVConstants.META_DB_TABLES,key.getBytes(),jsonVal.getBytes());
            rocksdbInstance.createCF(KVConstants.DATA_NODE,key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }
}

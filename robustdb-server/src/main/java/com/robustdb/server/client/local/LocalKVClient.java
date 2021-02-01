package com.robustdb.server.client.local;

import com.google.gson.Gson;
import com.robustdb.kv.constants.KVConstants;
import com.robustdb.kv.rocksdb.RocksdbInstance;
import com.robustdb.server.client.KVClient;
import com.robustdb.server.model.metadata.TableDef;
import org.rocksdb.RocksDBException;

import java.util.Map;


public class LocalKVClient implements KVClient {
    private RocksdbInstance rocksdbInstance = new RocksdbInstance();
    private static Gson gson = new Gson();

    public void createTableMetaData(TableDef tableDef) {
        String key = tableDef.getTableName();
        String jsonVal = gson.toJson(tableDef);
        try {
            rocksdbInstance.putCfKeyValue(KVConstants.META_DATA, KVConstants.META_DB_TABLES, key.getBytes(), jsonVal.getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void createDataTable(String tableName) {
        try {
            rocksdbInstance.createCF(KVConstants.DATA_NODE, tableName);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void insertData(Map<String, String> kvs, String tableName) {
        for (Map.Entry<String, String> entry : kvs.entrySet()) {
            try {
                String key = entry.getKey();
                String value = entry.getValue();
                rocksdbInstance.putCfKeyValue(KVConstants.DATA_NODE, tableName, key.getBytes(), value.getBytes());
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }

    public byte[] getDataNodeData(String key, String tableName) {
        try {
            return rocksdbInstance.getCfRelValue(KVConstants.DATA_NODE, tableName, key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] getMetaData(String key, String tableName) {
        try {
            return rocksdbInstance.getCfRelValue(KVConstants.META_DATA, tableName, key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }
}

package com.robustdb.server.client.remote;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.robustdb.kv.constants.KVConstants;
import com.robustdb.kv.rocksdb.RocksdbInstance;
import com.robustdb.rheakv.client.RheaKVClient;
import com.robustdb.server.client.KVClient;
import com.robustdb.server.model.metadata.TableDef;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Map;


public class RemoteKVClient implements KVClient {
    private RheaKVClient rheaKVClient;
    public RemoteKVClient() {
        rheaKVClient= new RheaKVClient();
        rheaKVClient.init();
    }


    private static Gson gson = new Gson();

    public void createTableMetaData(TableDef tableDef) {
        String key = tableDef.getTableName();
        String jsonVal = gson.toJson(tableDef);
//        try {
//
//            rocksdbInstance.putCfKeyValue(KVConstants.META_DATA, KVConstants.META_DB_TABLES, key.getBytes(), jsonVal.getBytes());
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        }
    }

    public void createDataTable(String tableName) {
//        try {
//            rocksdbInstance.createCF(KVConstants.DATA_NODE, tableName);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        }
    }

    public void insertData(Map<String, String> kvs) {
//        for (Map.Entry<String, String> entry : kvs.entrySet()) {
//            try {
//                String key = entry.getKey();
//                String value = entry.getValue();
//                rocksdbInstance.putCfKeyValue(KVConstants.DATA_NODE, tableName, key.getBytes(), value.getBytes());
//            } catch (RocksDBException e) {
//                e.printStackTrace();
//            }
//        }
    }

    public byte[] getDataNodeData(String key) {
//        try {
//            return rocksdbInstance.getCfRelValue(KVConstants.DATA_NODE, tableName, key);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        }
        return null;
    }

    public byte[] getMetaData(String key, String tableName) {
//        try {
//            return rocksdbInstance.getCfRelValue(KVConstants.META_DATA, tableName, key);
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        }
        return null;
    }

    @Override
    public List<String> getSecondaryIndexesOnDataNode(String key) {
        return null;
    }

    @Override
    public boolean containsKeyInDataNode(String key) {
        return false;
    }

    @Override
    public Map<String,JsonObject> fullTableScan(Map<String, String> queryCondition, String prefix) {
        return null;
    }

    @Override
    public int fetchTableId() {
        return 0;
    }

}

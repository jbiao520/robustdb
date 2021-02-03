package com.robustdb.server.client.local;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.robustdb.kv.constants.KVConstants;
import com.robustdb.kv.rocksdb.RocksdbInstance;
import com.robustdb.server.client.KVClient;
import com.robustdb.server.model.metadata.TableDef;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

    public void insertData(Map<String, String> kvs) {
        for (Map.Entry<String, String> entry : kvs.entrySet()) {
            try {
                byte[] key = entry.getKey().getBytes();
                byte[] value = entry.getValue() == null ? new byte[0] : entry.getValue().getBytes();
                rocksdbInstance.putCfKeyValue(KVConstants.DATA_NODE, "default", key, value);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }

    public byte[] getDataNodeData(String key) {
        try {
            return rocksdbInstance.getCfRelValue(KVConstants.DATA_NODE, "default", key);
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

    public List<String> getSecondaryIndexesOnDataNode(String key) {
        List<String> keyList = new ArrayList<>();
        RocksIterator iterator = rocksdbInstance.getCfAllValues(KVConstants.DATA_NODE, "default");
        iterator.seek(key.getBytes());
        while (iterator.isValid()) {
            String secIndexKey = new String(iterator.key());
            if (secIndexKey.startsWith(key)) {
                keyList.add(secIndexKey);
                iterator.next();
            } else {
                break;
            }

        }
        iterator.close();

        return keyList;
    }

    @Override
    public boolean containsKeyInDataNode(String key) {
        try {
            return rocksdbInstance.getCfRelValue(KVConstants.DATA_NODE, "default", key) != null;
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public List<JsonObject> fullTableScan(Map<String, String> queryConditions, String prefix) {
        List<JsonObject> rowList = new ArrayList<>();
        RocksIterator iterator = rocksdbInstance.getCfAllValues(KVConstants.DATA_NODE, "default");
        iterator.seek(prefix.getBytes());
        String idxPrefix = prefix + prefix;
        while (iterator.isValid()) {
            String key = new String(iterator.key());

            if (key.startsWith(prefix) && !key.startsWith(idxPrefix)) {
                String records = new String(iterator.value());
                JsonObject jsonObject = gson.fromJson(records, JsonObject.class);
                boolean isQualified = true;
                for (Map.Entry<String, String> kv : queryConditions.entrySet()) {
                    String condition = kv.getKey();
                    String value = kv.getValue();
                    String recordValue = jsonObject.get(condition).getAsString();
                    if (!recordValue.equals(value)) {
                        isQualified = false;
                        break;
                    }
                }
                if (isQualified) {
                    rowList.add(jsonObject);
                }
                iterator.next();
            } else {
                break;
            }


        }
        iterator.close();
        return rowList;
    }
}

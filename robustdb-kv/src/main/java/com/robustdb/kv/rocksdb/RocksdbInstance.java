package com.robustdb.kv.rocksdb;

import com.robustdb.kv.constants.KVConstants;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RocksdbInstance {

    private static final Map<String, RocksDB> dbInstances = new HashMap<>();
    private static final Map<String, Map<String, ColumnFamilyHandle>> dbCfHandleMap = new HashMap<>();
    private static final Map<String, WriteBatch> writeBatches = new HashMap<>();


    public void initMetaDataRocksDB() throws RocksDBException {
        RocksDB.loadLibrary();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        RocksDB db;
        Map<String, ColumnFamilyHandle> cfHandleMap = new HashMap<>();
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor(KVConstants.META_DB_TABLES.getBytes(), cfOpts)
            );
            DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setMaxTotalWalSize(128 << 20);
            // a factory method that returns a RocksDB instance
            db = RocksDB.open(options, KVConstants.META_DB_PATH, cfDescriptors, columnFamilyHandles);
            for (int i = 0; i < cfDescriptors.size(); i++) {
                ColumnFamilyHandle cfh = columnFamilyHandles.get(i);
                cfHandleMap.put(new String(cfh.getName(), StandardCharsets.UTF_8), cfh);
            }
            dbInstances.put(KVConstants.META_DATA, db);
            dbCfHandleMap.put(KVConstants.META_DATA, cfHandleMap);
        }
    }
    public void initDataNodeRocksDB(List<String> tableNames) throws RocksDBException {
        RocksDB.loadLibrary();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        RocksDB db;
        Map<String, ColumnFamilyHandle> cfHandleMap = new HashMap<>();
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            for (String tableName : tableNames) {
                cfDescriptors.add(new ColumnFamilyDescriptor(tableName.getBytes(), cfOpts));
            }
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
            DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setMaxTotalWalSize(128 << 20);
            // a factory method that returns a RocksDB instance
            db = RocksDB.open(options, KVConstants.DATA_DB_PATH, cfDescriptors, columnFamilyHandles);
            for (int i = 0; i < cfDescriptors.size(); i++) {
                ColumnFamilyHandle cfh = columnFamilyHandles.get(i);
                cfHandleMap.put(new String(cfh.getName(), StandardCharsets.UTF_8), cfh);
            }
            dbInstances.put(KVConstants.DATA_NODE, db);
            dbCfHandleMap.put(KVConstants.DATA_NODE, cfHandleMap);
        }
    }



    public byte[] getCfRelValue(String dbKey, String cf, String key) throws RocksDBException {
        RocksDB db = dbInstances.get(dbKey);
        Map<String, ColumnFamilyHandle> cfHandleMap = dbCfHandleMap.get(dbKey);
        if (db == null || cfHandleMap == null) {
            return null;
        }
        return db.get(cfHandleMap.get(cf), key.getBytes());
    }

    public RocksIterator getCfAllValues(String dbKey, String cf) {
        RocksDB db = dbInstances.get(dbKey);
        Map<String, ColumnFamilyHandle> cfHandleMap = dbCfHandleMap.get(dbKey);
        if (db == null || cfHandleMap == null) {
            return null;
        }
        return db.newIterator(cfHandleMap.get(cf));
    }

    public void putCfKeyValue(String dbKey, String cf, final byte[] key, final byte[] value) throws RocksDBException {
        WriteBatch writeBatch = writeBatches.get(dbKey);
        Map<String, ColumnFamilyHandle> cfHandleMap = dbCfHandleMap.get(dbKey);
        writeBatch.put(cfHandleMap.get(cf), key, value);
    }

    public void deleteCfKeyValue(String dbKey, String cf, final byte[] key) throws RocksDBException {
        WriteBatch writeBatch = writeBatches.get(dbKey);
        Map<String, ColumnFamilyHandle> cfHandleMap = dbCfHandleMap.get(dbKey);
        writeBatch.delete(cfHandleMap.get(cf), key);
    }


    public void flushToRocksDB(String dbKey) throws RocksDBException {
        RocksDB db = dbInstances.get(dbKey);
        WriteBatch writeBatch = writeBatches.get(dbKey);
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(true);
        db.write(writeOptions, writeBatch);
        writeBatch.clear();
    }
}


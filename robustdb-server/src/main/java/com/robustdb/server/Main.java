package com.robustdb.server;


import com.google.gson.Gson;
import com.robustdb.kv.constants.KVConstants;
import com.robustdb.kv.rocksdb.RocksdbInstance;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.sql.def.DefinitionCache;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        initKVStore();
        startServer();
    }

    private static void initKVStore() {
        try {
            RocksdbInstance rocksdbInstance = new RocksdbInstance();
            List<String> tableNames = new ArrayList();
            rocksdbInstance.initMetaDataRocksDB();
            RocksIterator rocksIterator = rocksdbInstance.getCfAllValues(KVConstants.META_DATA,KVConstants.META_DB_TABLES);
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                String tableName = new String(rocksIterator.key());
                TableDef tableDef = new Gson().fromJson(new String(rocksIterator.value()),TableDef.class);
                tableNames.add(tableName);
                if(tableDef.isIndexTable()){
                    String rawTableName = tableName.split("_")[0];
                    DefinitionCache.addIndexTableDef(rawTableName,tableDef);
                }else{
                    DefinitionCache.addTableDef(tableName,tableDef);
                }
            }
            DefinitionCache.dumpCaches();
            rocksdbInstance.initDataNodeRocksDB(tableNames);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private static void startServer() throws IOException {
        ServerSocket s = new ServerSocket(3060);
        System.out.println("Server Started");
        try {
            while(true) {
                // Blocks until a connection occurs:
                Socket socket = s.accept();
                try {
                    new ServeOneJabber(socket);
                } catch(IOException e) {
                    // If it fails, close the socket,
                    // otherwise the thread will close it:
                    socket.close();
                }
            }
        } finally {
            s.close();
        }
    }
}

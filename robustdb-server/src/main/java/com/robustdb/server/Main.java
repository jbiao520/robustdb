package com.robustdb.server;


import com.robustdb.kv.constants.KVConstants;
import com.robustdb.kv.rocksdb.RocksdbInstance;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {


        try {
            RocksdbInstance rocksdbInstance = new RocksdbInstance();
            List<String> tableNames = new ArrayList<String>();
            rocksdbInstance.initMetaDataRocksDB();
            RocksIterator rocksIterator = rocksdbInstance.getCfAllValues(KVConstants.META_DATA,KVConstants.META_DB_TABLES);
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                String key = new String(rocksIterator.key());
                tableNames.add(key);
            }
            rocksdbInstance.initDataNodeRocksDB(tableNames);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

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

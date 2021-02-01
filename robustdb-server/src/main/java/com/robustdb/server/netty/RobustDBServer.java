package com.robustdb.server.netty;

import com.google.gson.Gson;
import com.robustdb.kv.constants.KVConstants;
import com.robustdb.kv.rocksdb.RocksdbInstance;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.sql.def.DefinitionCache;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RobustDBServer {

    public static int DEFAULT_PORT = 3307;
    public static void main(String[] args) throws Exception {
        log.info("Starting netty server");

        EventLoopGroup bossGroup = new NioEventLoopGroup(); // boss
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // worker

        try {
            ServerBootstrap b = new ServerBootstrap();

            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ServerChannelInitializer())
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            initKVStore();
            ChannelFuture f = b.bind(DEFAULT_PORT).sync();
            log.info("Netty server started with port:{}",DEFAULT_PORT);
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

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
}
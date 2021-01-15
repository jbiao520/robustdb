package com.robustdb.server.sql.def;

import com.robustdb.server.model.metadata.TableDef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefinitionCache {
    private static final Map<String, TableDef> tableDefCache = new HashMap<>();
    private static final Map<String, List<TableDef>> indexTableDefCache = new HashMap<>();


    public static void addTableDef(String tableName, TableDef tableDef){
        tableDefCache.put(tableName,tableDef);
    }

    public static void removeTableDef(String tableName){
        tableDefCache.remove(tableName);
    }

    public static void addIndexTableDef(String tableName, TableDef tableDef){
        if(indexTableDefCache.get(tableName)!=null){
            indexTableDefCache.get(tableName).add(tableDef);
        }else{
            List<TableDef> tableDefs = new ArrayList<>();
            tableDefs.add(tableDef);
            indexTableDefCache.put(tableName,tableDefs);
        }

    }

    public static void removeIndexTableDef(String tableName){
        indexTableDefCache.remove(tableName);
    }

    public static TableDef getTableDef(String tableName){
        return tableDefCache.get(tableName);
    }

    public static List<TableDef> getIndexTableDefs(String tableName){
        return indexTableDefCache.get(tableName);
    }

    public static void dumpCaches(){
        System.out.println("++++++++++++++++++++++++++++++++++Table inited++++++++++++++++++++++++++++++++++");
        tableDefCache.forEach((name,tableDefCache)->{
            System.out.println("+++++++++++++++++"+name+"+++++++++++++++++");
            System.out.println(tableDefCache);
        });
        System.out.println("++++++++++++++++++++++++++++++++++Table inited++++++++++++++++++++++++++++++++++");
        System.out.println("++++++++++++++++++++++++++++++++++Indexes inited++++++++++++++++++++++++++++++++++");
        indexTableDefCache.forEach((name,tableDefCache)->{
            System.out.println("+++++++++++++++++"+name+"+++++++++++++++++");
            System.out.println(tableDefCache);
        });
        System.out.println("++++++++++++++++++++++++++++++++++Indexes inited++++++++++++++++++++++++++++++++++");
    }
}

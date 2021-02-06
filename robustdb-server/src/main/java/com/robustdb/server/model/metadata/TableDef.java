package com.robustdb.server.model.metadata;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Setter
@Getter
@Builder
@ToString
public class TableDef {
    private String rawTableDef;
    private String tableName;
    private ColumnDef primaryKeyDef;
    private String primaryKey;
//    private List<ColumnDef> columnDefList;
    private Map<String, ColumnDef> columnDefMap;
    private String comment;
    private boolean isIndexTable;
    private boolean isUnique;
    private int tableId;
}

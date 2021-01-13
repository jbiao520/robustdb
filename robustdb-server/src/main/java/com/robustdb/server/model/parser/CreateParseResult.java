package com.robustdb.server.model.parser;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import lombok.*;

import java.util.List;

@Setter
@Getter
@Builder
@ToString
public class CreateParseResult implements ParseResult{
    private String rawTableDef;
    private String tableName;
    private List<SQLColumnDefinition> columns;
    private List<MySqlTableIndex> indexList;
}

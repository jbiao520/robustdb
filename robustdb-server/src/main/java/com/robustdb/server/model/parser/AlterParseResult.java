package com.robustdb.server.model.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.robustdb.server.enums.AlterType;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@Builder
@ToString
public class AlterParseResult implements ParseResult{
    private String tableName;
    private String indexName;
    private String rawSQL;
    private List<SQLIdentifierExpr> columns;
    private AlterType alterType;
}

package com.robustdb.server.model.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@Builder
@ToString
public class UpdateParseResult implements ParseResult{
    private String tableName;
    private List<SQLUpdateSetItem> updateSetItems;
    private SQLExpr where;
}

package com.robustdb.server.model.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@Builder
@ToString
public class SelectParseResult implements ParseResult{
    private String tableName;
    private SQLExpr where;
    private List<SQLSelectItem> selectList;
}

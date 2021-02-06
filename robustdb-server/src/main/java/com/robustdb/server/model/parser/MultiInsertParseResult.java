package com.robustdb.server.model.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Setter
@Getter
@Builder
@ToString
public class MultiInsertParseResult implements ParseResult{
    private List<InsertParseResult> insertParseResult;
}

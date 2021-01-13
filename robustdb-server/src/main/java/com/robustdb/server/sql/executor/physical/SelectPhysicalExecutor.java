package com.robustdb.server.sql.executor.physical;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.robustdb.server.model.parser.CreateParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.SelectParseResult;

public class SelectPhysicalExecutor extends AbstractPhysicalExecutor{

    @Override
    protected void execute(ParseResult parseResult) {
        SelectParseResult result = (SelectParseResult)parseResult;
        SQLExpr where = result.getWhere();

    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof SelectParseResult;
    }
}

package com.robustdb.server.sql.executor.physical;

import com.robustdb.server.model.parser.CreateParseResult;
import com.robustdb.server.model.parser.InsertParseResult;
import com.robustdb.server.model.parser.ParseResult;

public class InsertPhysicalExecutor extends AbstractPhysicalExecutor{

    @Override
    protected void execute(ParseResult parseResult) {

    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof InsertParseResult;
    }
}

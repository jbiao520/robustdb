package com.robustdb.server.sql.executor.physical;

import com.robustdb.server.model.parser.CreateParseResult;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.UpdateParseResult;

public class UpdatePhysicalExecutor extends AbstractPhysicalExecutor{

    @Override
    protected void execute(ParseResult parseResult) {

    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof UpdateParseResult;
    }
}

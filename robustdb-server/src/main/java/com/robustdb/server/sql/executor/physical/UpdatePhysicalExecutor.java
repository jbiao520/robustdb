package com.robustdb.server.sql.executor.physical;

import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.model.parser.UpdateParseResult;
import com.robustdb.server.sql.executor.ExecutorResult;

public class UpdatePhysicalExecutor extends AbstractPhysicalExecutor{

    @Override
    protected ExecutorResult execute(ParseResult parseResult) {
        return null;
    }

    @Override
    protected boolean compatible(ParseResult parseResult) {
        return parseResult instanceof UpdateParseResult;
    }
}

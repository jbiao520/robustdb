package com.robustdb.server.sql.executor.physical;

import com.google.gson.Gson;
import com.robustdb.server.client.KVClient;
import com.robustdb.server.client.local.LocalKVClient;
import com.robustdb.server.enums.ConstraintType;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.ParseResult;

import java.util.List;

public abstract class AbstractPhysicalExecutor {

    protected AbstractPhysicalExecutor nextExecutor;
    protected Gson gson = new Gson();
    protected KVClient kvClient = new LocalKVClient();
    public void setNextExecutor(AbstractPhysicalExecutor sqlExectuor){
        this.nextExecutor = sqlExectuor;
    }
    public void executePlan(ParseResult parseResult) {
        if (compatible(parseResult)) {
            execute(parseResult);
        }
        if (nextExecutor != null) {
            nextExecutor.executePlan(parseResult);
        }
    }

    abstract protected void execute(ParseResult parseResult);

    abstract protected boolean compatible(ParseResult parseResult);

    protected List<ConstraintType> getConstriants(TableDef tableDef, String columnName) {
        return tableDef.getColumnDefMap().get(columnName).getConstraintTypes();
    }
}

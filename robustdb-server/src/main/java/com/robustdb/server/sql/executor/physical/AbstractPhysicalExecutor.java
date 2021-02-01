package com.robustdb.server.sql.executor.physical;

import com.google.gson.Gson;
import com.robustdb.server.client.KVClient;
import com.robustdb.server.client.local.LocalKVClient;
import com.robustdb.server.enums.ConstraintType;
import com.robustdb.server.model.metadata.TableDef;
import com.robustdb.server.model.parser.ParseResult;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.sql.executor.ExecutorResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

public abstract class AbstractPhysicalExecutor {

    protected AbstractPhysicalExecutor nextExecutor;
    protected Gson gson = new Gson();
    protected KVClient kvClient = new LocalKVClient();
    public void setNextExecutor(AbstractPhysicalExecutor sqlExectuor){
        this.nextExecutor = sqlExectuor;
    }
    public ExecutorResult executePlan(ParseResult parseResult) {
        if (compatible(parseResult)) {
            return execute(parseResult);
        }
        if (nextExecutor != null) {
            return  nextExecutor.executePlan(parseResult);
        }
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(OkPacket.OK);
        return ExecutorResult.builder().byteBuf(byteBuf).build();
    }

    abstract protected ExecutorResult execute(ParseResult parseResult);

    abstract protected boolean compatible(ParseResult parseResult);

    protected List<ConstraintType> getConstriants(TableDef tableDef, String columnName) {
        return tableDef.getColumnDefMap().get(columnName).getConstraintTypes();
    }
}

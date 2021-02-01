package com.robustdb.server.sql.executor;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@Builder
@ToString
public class ExecutorResult {
    private ByteBuf byteBuf;
}

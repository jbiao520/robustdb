package com.robustdb.server.netty.handlers;

import com.robustdb.server.enums.ConnLifeCycle;
import com.robustdb.server.protocol.mysql.AuthPacket;
import com.robustdb.server.protocol.mysql.HandshakeV10Packet;
import com.robustdb.server.protocol.mysql.OkPacket;
import com.robustdb.server.util.Capabilities;
import com.robustdb.server.util.CharsetUtil;
import com.robustdb.server.util.RandomUtil;
import com.robustdb.server.util.Versions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@ChannelHandler.Sharable
public class AuthServerHandler extends ChannelInboundHandlerAdapter {
    private static Map<ChannelId, ConnLifeCycle> channelStatusMap = new HashMap<>();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        if (channelStatusMap.containsKey(ctx.channel().id())) {
            log.info("Channel id already existed in the channelStatusMap, id:{}", ctx.channel().id());
        } else {
            log.info("New connection detected, start the handshake process, id:{}", ctx.channel().id());
            byte[] rand1 = RandomUtil.randomBytes(8);
            byte[] rand2 = RandomUtil.randomBytes(12);
            // 保存认证数据
            byte[] seed = new byte[rand1.length + rand2.length];
            System.arraycopy(rand1, 0, seed, 0, rand1.length);
            System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
            HandshakeV10Packet hs = new HandshakeV10Packet();
            hs.packetId = 0;
            hs.protocolVersion = Versions.PROTOCOL_VERSION;
            hs.serverVersion = Versions.SERVER_VERSION;
            hs.threadId = 1;
            hs.seed = rand1;
            hs.serverCapabilities = getServerCapabilities();
            hs.serverCharsetIndex = (byte) (CharsetUtil.getIndex("utf8") & 0xff);
            hs.serverStatus = 2;
            hs.restOfScrambleBuff = rand2;
            ctx.write(hs.write());
            ctx.flush();
            log.info("HandshakePacket flushed to channel id:{}", ctx.channel().id());
            channelStatusMap.put(ctx.channel().id(), ConnLifeCycle.HANDSHAKE);
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        log.info("Channel registered with channel id:{}", ctx.channel().id());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) {
        if (channelStatusMap.get(ctx.channel().id()) == ConnLifeCycle.HANDSHAKE) {
            log.info("Receive auth packet from channel id:{}", ctx.channel().id());
            ByteBuf buf = (ByteBuf) obj;
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            System.out.println(bytes[4]);
            AuthPacket msg = new AuthPacket();
            msg.read(bytes);
            ByteBuf bufferOut = Unpooled.buffer();
            bufferOut.writeBytes(OkPacket.AUTH_OK);
            log.info("Write OK packet to channel id:{}", ctx.channel().id());
            ctx.write(bufferOut);
        } else if (channelStatusMap.get(ctx.channel().id()) == ConnLifeCycle.COMMAND) {
            log.info("Detected COMMAND phase channel id:{}, forward to next handler.", ctx.channel().id());
            ctx.fireChannelRead(obj);
        } else {
            log.info("Unknown phase channel id:{}, forward to next handler.", ctx.channel().id());
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        channelStatusMap.put(ctx.channel().id(), ConnLifeCycle.COMMAND);
        log.info("Channel read completed for handshake with channel id:{}", ctx.channel().id());
        ctx.flush();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        log.info("Channel Unregistered for channel id:{}", ctx.channel().id());
        channelStatusMap.remove(ctx.channel().id());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("Channel Inactive for channel id:{}", ctx.channel().id());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("exceptionCaught channel id:{}, msg:{}", ctx.channel().id(), cause);
        ctx.close();
    }

    protected int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = false;
        if (usingCompress) {
            flag |= Capabilities.CLIENT_COMPRESS;
        }

        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        boolean useHandshakeV10 = true;
        if (useHandshakeV10) {
            flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        }
        return flag;
    }
}
package com.robustdb.server.protocol.mysql;

import com.robustdb.server.util.BufferUtil;
import com.robustdb.server.util.Capabilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


public class HandshakeV10Packet extends MySQLPacket {
    private static final byte[] FILLER_10 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    private static final byte[] DEFAULT_AUTH_PLUGIN_NAME = "mysql_native_password".getBytes();

    public byte protocolVersion;
    public byte[] serverVersion;
    public long threadId;
    public byte[] seed; // auth-plugin-data-part-1
    public int serverCapabilities;
    public byte serverCharsetIndex;
    public int serverStatus;
    public byte[] restOfScrambleBuff; // auth-plugin-data-part-2
    public byte[] authPluginName = DEFAULT_AUTH_PLUGIN_NAME;

    public ByteBuf write() {

        ByteBuf buffer = Unpooled.buffer();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(protocolVersion);
        BufferUtil.writeWithNull(buffer, serverVersion);
        BufferUtil.writeUB4(buffer, threadId);
        buffer.writeBytes(seed);
        buffer.writeByte((byte)0); // [00] filler
        BufferUtil.writeUB2(buffer, serverCapabilities); // capability flags (lower 2 bytes)
        buffer.writeByte(serverCharsetIndex);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, (serverCapabilities >> 16)); // capability flags (upper 2 bytes)
        if((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            if(restOfScrambleBuff.length <= 13) {
                buffer.writeByte((byte) (seed.length + 13));
            } else {
                buffer.writeByte((byte) (seed.length + restOfScrambleBuff.length));
            }
        } else {
            buffer.writeByte((byte) 0);
        }
        buffer.writeBytes(FILLER_10);
        if((serverCapabilities & Capabilities.CLIENT_SECURE_CONNECTION) != 0) {
            buffer.writeBytes(restOfScrambleBuff);
            // restOfScrambleBuff.length always to be 12
            if(restOfScrambleBuff.length < 13) {
                for(int i = 13 - restOfScrambleBuff.length; i > 0; i--) {
                    buffer.writeByte((byte)0);
                }
            }
        }
        if((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            BufferUtil.writeWithNull(buffer, authPluginName);
        }
        return buffer;
    }

    @Override
    public int calcPacketSize() {
        int size = 1; // protocol version
        size += (serverVersion.length + 1); // server version
        size += 4; // connection id
        size += seed.length;
        size += 1; // [00] filler
        size += 2; // capability flags (lower 2 bytes)
        size += 1; // character set
        size += 2; // status flags
        size += 2; // capability flags (upper 2 bytes)
        size += 1;
        size += 10; // reserved (all [00])
        if((serverCapabilities & Capabilities.CLIENT_SECURE_CONNECTION) != 0) {
            // restOfScrambleBuff.length always to be 12
            if(restOfScrambleBuff.length <= 13) {
                size += 13;
            } else {
                size += restOfScrambleBuff.length;
            }
        }
        if((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            size += (authPluginName.length + 1); // auth-plugin name
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL HandshakeV10 Packet";
    }

}
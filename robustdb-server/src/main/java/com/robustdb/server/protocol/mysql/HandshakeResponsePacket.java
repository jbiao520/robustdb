package com.robustdb.server.protocol.mysql;

import com.robustdb.server.util.BufferUtil;
import com.robustdb.server.util.Capabilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


public class HandshakeResponsePacket extends MySQLPacket {
    private static final byte[] FILLER_23 = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static final byte[] DEFAULT_AUTH_PLUGIN_NAME = "mysql_native_password".getBytes();

    public long client_flag;
    public long max_packet_size;
    public byte serverCharsetIndex;
    public byte[] username;
    public byte auth_response_length;
    public byte[] auth_response;
    public byte[] database;
    public byte[] client_plugin_name;
    public byte zstd_compression_level;
    public int serverCapabilities;

    public ByteBuf write() {

        ByteBuf buffer = Unpooled.buffer();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        BufferUtil.writeUB4(buffer, client_flag);
        BufferUtil.writeUB4(buffer, max_packet_size);
        buffer.writeByte(serverCharsetIndex);
        buffer.writeBytes(FILLER_23);
        BufferUtil.writeWithNull(buffer, username);
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            BufferUtil.writeWithLength(buffer, auth_response);
        } else {
            buffer.writeByte(auth_response_length);
            BufferUtil.writeWithLength(buffer, auth_response);
        }
        if ((serverCapabilities & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) {
            BufferUtil.writeWithNull(buffer, database);
        }
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            BufferUtil.writeWithNull(buffer, client_plugin_name);
        }
        if ((serverCapabilities & Capabilities.CLIENT_CONNECT_ATTRS) != 0) {

        }
        buffer.writeByte(zstd_compression_level);
        return buffer;
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        client_flag = mm.readUB4();
        max_packet_size = mm.readUB4();
        mm.move(23);
        username = mm.readBytesWithNull();
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            auth_response = mm.readBytesWithLength();
        } else {
            auth_response_length = mm.read();
            auth_response=mm.readBytes(auth_response_length);
        }
        if ((serverCapabilities & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) {
            database = mm.readBytesWithNull();
        }
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            client_plugin_name = mm.readBytesWithNull();
        }
        if ((serverCapabilities & Capabilities.CLIENT_CONNECT_ATTRS) != 0) {

        }
        zstd_compression_level = mm.read();
    }

    @Override
    public int calcPacketSize() {
        int size = 0;
        size += 4; // client_flag
        size += 4; // max_packet_size id
        size += 1; // character set
        size += 23; // [00] filler
        size += (username.length + 1); // capability flags (lower 2 bytes)
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            size += auth_response.length;
        } else {
            size += auth_response.length + 1;
        }
        if ((serverCapabilities & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) {
            size += database.length + 1;
        }
        if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            size += client_plugin_name.length + 1;
        }
        size += 1;
        return size;
    }



    @Override
    protected String getPacketInfo() {
        return "MySQL HandshakeResponse Packet";
    }

}
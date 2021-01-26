package com.robustdb.server.util;

public abstract class Versions {

    /**协议版本**/
    public static final byte PROTOCOL_VERSION = 10;

    /**服务器版本**/
    public static byte[] SERVER_VERSION = "5.6.29-mycat-1.6.7.5-release-20200428154739".getBytes();

    public static void setServerVersion(String version) {
        byte[] mysqlVersionPart = version.getBytes();
        int startIndex;
        for (startIndex = 0; startIndex < SERVER_VERSION.length; startIndex++) {
            if (SERVER_VERSION[startIndex] == '-')
                break;
        }

        // 重新拼接mycat version字节数组
        byte[] newMycatVersion = new byte[mysqlVersionPart.length + SERVER_VERSION.length - startIndex];
        System.arraycopy(mysqlVersionPart, 0, newMycatVersion, 0, mysqlVersionPart.length);
        System.arraycopy(SERVER_VERSION, startIndex, newMycatVersion, mysqlVersionPart.length,
                SERVER_VERSION.length - startIndex);
        SERVER_VERSION = newMycatVersion;
    }
}

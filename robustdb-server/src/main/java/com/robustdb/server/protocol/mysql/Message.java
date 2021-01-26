package com.robustdb.server.protocol.mysql;

/**
 * Message is the content of a full single message (native protocol packet or protobuf message),
 * independent from on-wire splitting, communicated with the server.
 */
public interface Message {

    /**
     * Returns the array of bytes this Buffer is using to read from.
     *
     * @return byte array being read from
     */
    byte[] getByteBuffer();

    /**
     * Returns the current position to write to/ read from
     *
     * @return the current position to write to/ read from
     */
    int getPosition();

}

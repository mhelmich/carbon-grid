/*
 * Copyright 2017 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbon.grid;

import io.netty.buffer.ByteBuf;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

abstract class Message implements Persistable, Cloneable {

    // NEVER CHANGE THE ORDINALS!!!
    // first parameter is the ordinal
    // the second parameter is the message byte size after reading the first type byte
    enum MessageType {
        PUT((byte)0, 999),
        GET((byte)1, 12),             // msgId + line = 4 + 8 = 12
        ACK((byte)2, 4),              // msgId = 4
        INVALIDATE((byte)3, 999),
        INVALIDATE_ACK((byte)4, 999),
        BACKUP((byte)5, 999),
        BACUP_ACK((byte)6, 999);

        private final static Map<Byte, MessageType> lookUp = new HashMap<>(MessageType.values().length);
        static {
            Set<Byte> ordinals = new HashSet<>(MessageType.values().length);
            for (MessageType type : MessageType.values()) {
                lookUp.put(type.ordinal, type);

                if (!ordinals.add(type.ordinal)) {
                    throw new RuntimeException("Can't add ordinal " + type.ordinal + " twice!");
                }
            }
        }

        final byte ordinal;
        final int metadataByteSize;
        MessageType(byte ordinal, int metadataByteSize) {
            this.ordinal = ordinal;
            this.metadataByteSize = metadataByteSize;
        }

        static MessageType fromByte(byte b) {
            return lookUp.get(b);
        }
    }

    private final static AtomicInteger messageIdGenerator = new AtomicInteger(Integer.MIN_VALUE);

    final MessageType type;
    private int messageId;
    // this field has two semantics
    // 1. in a request it is the sender
    // 2. in a response it's the receiver
    short node;

    private Message(MessageType type, Message inResponseTo) {
        this(type, inResponseTo.messageId, inResponseTo.node);
    }

    private Message(MessageType type, int messageId, short node) {
        this.type = type;
        this.messageId = messageId;
        this.node = node;
    }

    private static int newMessageId() {
        int messageId = messageIdGenerator.incrementAndGet();
        if (messageId == Integer.MAX_VALUE) {
            messageIdGenerator.set(Integer.MIN_VALUE);
        }
        return messageId;
    }

    @Deprecated
    static Message newMessage(MessageType type, ByteBuf bites) {
        switch (type) {
//            case GET:
//                return new GET(bites.readShort(), bites.readLong());
//            case ACK:
//                return new ACK();
            default:
                throw new RuntimeException("Unknown message type " + type);
        }
    }

    @Override
    public int byteSize() {
        return calcByteSize();
    }

    abstract int calcByteSize();

    static abstract class Request extends Message {
        private Request(MessageType type, int messageId, short node) {
            super(type, messageId, node);
        }
    }

    static abstract class Response extends Message {
        private Response(MessageType type, Message inResponseTo) {
            super(type, inResponseTo);
        }
    }

    static Message newResponseTo(MessageType responseType, Message inResponseTo) {
        switch (responseType) {
            case ACK:
                return new ACK(inResponseTo);
            default:
                throw new RuntimeException("Unknown message type " + responseType);
        }
    }

    static class GET extends Request {
        private long lineId;

        GET(short node, long lineId) {
            super(MessageType.GET, Message.newMessageId(), node);
            this.lineId = lineId;
        }

        // convenience ctor
        // I got tired of the constant casting
        GET(int node, int lineId) {
            this((short)node, (long)lineId);
        }

        long getLineId() {
            return lineId;
        }

        @Override
        int calcByteSize() {
            return 0;
        }

        @Override
        public void write(OutputStream out) {

        }

        @Override
        public void read(InputStream in) {

        }
    }

    static class ACK extends Response {
        ACK(Message inResponseTo) {
            super(MessageType.ACK, inResponseTo);
        }

        @Override
        int calcByteSize() {
            return 0;
        }

        @Override
        public void write(OutputStream out) {

        }

        @Override
        public void read(InputStream in) {

        }
    }

}

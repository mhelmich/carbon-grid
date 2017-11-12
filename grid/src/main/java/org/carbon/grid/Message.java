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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

abstract class Message implements Persistable {

    // NEVER CHANGE THE ORDINALS!!!
    // first parameter is the ordinal
    // the second parameter is the message byte size after reading the first type byte
    enum MessageType {
        PUT((byte)0),
        GET((byte)1),
        ACK((byte)2),
        INVALIDATE((byte)3),
        INVALIDATE_ACK((byte)4),
        BACKUP((byte)5),
        BACUP_ACK((byte)6);

        private final static Map<Byte, MessageType> byteToType = new HashMap<>(MessageType.values().length);
        static {
            Set<Byte> ordinals = new HashSet<>(MessageType.values().length);
            for (MessageType type : MessageType.values()) {
                byteToType.put(type.ordinal, type);

                if (!ordinals.add(type.ordinal)) {
                    throw new RuntimeException("Can't add ordinal " + type.ordinal + " twice!");
                }
            }
        }

        final byte ordinal;
        MessageType(byte ordinal) {
            this.ordinal = ordinal;
        }

        static MessageType fromByte(byte b) {
            return byteToType.get(b);
        }
    }

    static Request getRequestForType(MessageType type) {
        switch (type) {
            case GET:
                return new GET();
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private final static AtomicInteger messageIdGenerator = new AtomicInteger(Integer.MIN_VALUE);

    final MessageType type;
    private int messageId;
    // this field has two semantics
    // 1. in a request it is the sender
    // 2. in a response it's the receiver
    short sender;

    private Message(MessageType type, Message inResponseTo) {
        this(type, inResponseTo.messageId, inResponseTo.sender);
    }

    private Message(MessageType type, int messageId, short sender) {
        this.type = type;
        this.messageId = messageId;
        this.sender = sender;
    }

    private static int newMessageId() {
        int messageId = messageIdGenerator.incrementAndGet();
        if (messageId == Integer.MAX_VALUE) {
            messageIdGenerator.set(Integer.MIN_VALUE);
        }
        return messageId;
    }

    @Override
    public int byteSize() {
        return calcByteSize();
    }

    int calcByteSize() {
        return 1     // message type byte
             + 2     // sender short
               ;
    }

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

    static class GET extends Request {
        private long lineId;

        GET() {
            this(-1, -1);
        }

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
            return super.calcByteSize()
                    + 8; // lineId long
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(MessageType.GET.ordinal);
            out.writeShort(sender);
            out.writeLong(lineId);
        }

        @Override
        public void read(DataInput in) throws IOException {
            sender = in.readShort();
            lineId = in.readLong();
        }
    }

    static class ACK extends Response {
        ACK(Message inResponseTo) {
            super(MessageType.ACK, inResponseTo);
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(MessageType.ACK.ordinal);
            out.writeShort(sender);
        }

        @Override
        public void read(DataInput in) throws IOException {
            sender = in.readShort();
        }
    }

}

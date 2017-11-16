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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        BACUP_ACK((byte)6),
        RESEND((byte)7),
        GETX((byte)8),
        PUTX((byte)9),
        ;

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

    static Response getResponseForType(MessageType type) {
        switch (type) {
            case ACK:
                return new ACK();
            case PUT:
                return new PUT();
            default:
                throw new IllegalArgumentException("Unknown type " + type);
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

    final MessageType type;
    int messageId;
    // this field has two semantics
    // 1. in a request it is the sender
    // 2. in a response it's the receiver
    short sender;

    private Message(MessageType type, Request inResponseTo) {
        this(type, inResponseTo.messageId, inResponseTo.sender);
    }

    private Message(MessageType type) {
        this.type = type;
    }

    private Message(MessageType type, short sender) {
        this.type = type;
        this.sender = sender;
    }

    private Message(MessageType type, int messageId, short sender) {
        this.type = type;
        this.messageId = messageId;
        this.sender = sender;
    }

    @Override
    public int byteSize() {
        return calcByteSize();
    }

    int calcByteSize() {
        return 1     // message type byte
             + 4     // message id int
             + 2     // sender short
               ;
    }

    @Override
    public void write(MessageOutput out) throws IOException {
        out.writeByte(type.ordinal);
        out.writeInt(messageId);
        out.writeShort(sender);
    }

    @Override
    public void read(MessageInput in) throws IOException {
        messageId = in.readInt();
        sender = in.readShort();
    }

    @Override
    public String toString() {
        return "message type: " + type + " messageId: " + messageId + " sender: " + sender;
    }

    static abstract class Request extends Message {
        private Request(MessageType type, short node) {
            super(type, node);
        }
    }

    static abstract class Response extends Message {
        private Response(MessageType type) {
            super(type);
        }

        private Response(MessageType type, Request inResponseTo) {
            super(type, inResponseTo);
        }
    }

    static class GET extends Request {
        long lineId;

        GET() {
            super(MessageType.GET, (short)-1);
        }

        GET(short node, long lineId) {
            super(MessageType.GET, node);
            this.lineId = lineId;
        }

        // convenience ctor
        // I got tired of the constant casting
        GET(int node, int lineId) {
            this((short)node, (long)lineId);
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize()
                    + 8; // lineId long
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeLong(lineId);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            lineId = in.readLong();
        }

        @Override
        public String toString() {
            return super.toString() + " lineId: " + lineId;
        }
    }

    static class PUT extends Response {
        long lineId;
        int version;
        ByteBuf data;

        PUT() {
            super(MessageType.PUT);
        }

        PUT(Request inResponseTo, long lineId, int version, ByteBuf data) {
            super(MessageType.PUT, inResponseTo);
            this.lineId = lineId;
            this.version = version;
            this.data = data;
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize()
                    + 8                 // line id long
                    + 4                 // version number int
                    + data.capacity();  // buffer content
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeLong(lineId);
            out.writeInt(version);
            out.writeByteBuf(data.resetReaderIndex());
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            lineId = in.readLong();
            version = in.readInt();
            data = in.readByteBuf();
        }
    }

    static class ACK extends Response {
        ACK(Request inResponseTo) {
            super(MessageType.ACK, inResponseTo);
        }

        ACK() {
            super(MessageType.ACK);
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize();
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
        }
    }

    static class GETX extends Request {
        long lineId;

        GETX(short node) {
            super(MessageType.GETX, node);
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize() + 8;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeLong(lineId);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            lineId = in.readLong();
        }
    }

    static class PUTX extends Response {
        long lineId;
        int version;
        short parts;
        // variable and potentially unbounded size
        List<Short> sharers;
        ByteBuf data;

        PUTX() {
            super(MessageType.PUTX);
        }

        PUTX(Request inResponseTo) {
            super(MessageType.PUTX, inResponseTo);
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize()
                    + 8                    // line id long
                    + 4                    // version number int
                    + (2 * sharers.size()) //
                    + data.capacity();     // buffer content
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeLong(lineId);
            out.writeInt(version);
            out.writeShort(sharers.size());
            for (Short s : sharers) {
                out.writeShort(s);
            }
            out.writeByteBuf(data.resetReaderIndex());
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            lineId = in.readLong();
            version = in.readInt();
            short numSharers = in.readShort();
            sharers = new ArrayList<>(numSharers);
            for (int i = 0; i < numSharers; i++) {
                sharers.add(in.readShort());
            }
            data = in.readByteBuf();
        }
    }

}

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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
        OWNER_CHANGED((byte)10)
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

    static Message getMessageForType(MessageType type) {
        switch (type) {
            case ACK:
                return new ACK();
            case PUT:
                return new PUT();
            case PUTX:
                return new PUTX();
            case GET:
                return new GET();
            case GETX:
                return new GETX();
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    @Deprecated
    static Response getResponseForType(MessageType type) {
        switch (type) {
            case ACK:
                return new ACK();
            case PUT:
                return new PUT();
            case PUTX:
                return new PUTX();
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    @Deprecated
    static Request getRequestForType(MessageType type) {
        switch (type) {
            case GET:
                return new GET();
            case GETX:
                return new GETX();
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    final MessageType type;
    int messageId;
    short sender;

    private Message(MessageType type, Request inResponseTo, short sender) {
        this(type, inResponseTo.messageId, sender);
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

    abstract Message copy();

    static abstract class Request extends Message {
        private Request(MessageType type, short node) {
            super(type, node);
        }
    }

    static abstract class Response extends Message {
        private Response(MessageType type) {
            super(type);
        }

        private Response(MessageType type, Request inResponseTo, short sender) {
            super(type, inResponseTo, sender);
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

        @Override
        Message copy() {
            return new GET(this.sender, lineId);
        }
    }

    static class PUT extends Response {
        long lineId;
        int version;
        ByteBuf data;

        PUT() {
            super(MessageType.PUT);
        }

        PUT(Request inResponseTo, short sender, long lineId, int version, ByteBuf data) {
            super(MessageType.PUT, inResponseTo, sender);
            this.lineId = lineId;
            this.version = version;
            this.data = data;
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize()
                    + 8                 // line id long
                    + 4                 // version number int
                    + 4                 // bytebuf size
                    + data.capacity();  // buffer content
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeLong(lineId);
            out.writeInt(version);
            out.writeInt(data.capacity());
            out.writeByteBuf(data.resetReaderIndex());
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            lineId = in.readLong();
            version = in.readInt();
            int bytesToRead = in.readInt();
            data = in.readByteBuf(bytesToRead);
        }

        @Override
        Message copy() {
            throw new NotImplementedException();
        }
    }

    static class ACK extends Response {
        ACK(Request inResponseTo, short sender) {
            super(MessageType.ACK, inResponseTo, sender);
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

        @Override
        Message copy() {
            return new ACK();
        }
    }

    static class GETX extends Request {
        long lineId;

        GETX() {
            super(MessageType.GETX, (short)-1);
        }

        GETX(short node, long lineId) {
            super(MessageType.GETX, node);
            this.lineId = lineId;
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

        @Override
        Message copy() {
            throw new NotImplementedException();
        }
    }

    static class PUTX extends Response {
        long lineId;
        int version;
        // variable and potentially unbounded size
        Set<Short> sharers;
        ByteBuf data;

        PUTX() {
            super(MessageType.PUTX);
        }

        PUTX(Request inResponseTo, short sender) {
            super(MessageType.PUTX, inResponseTo, sender);
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize()
                    + 8                    // line id long
                    + 4                    // version number int
                    + 2                    // num sharers short
                    + (2 * sharers.size()) //
                    + 4                    // bytebuf size
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
            out.writeInt(data.capacity());
            out.writeByteBuf(data.resetReaderIndex());
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            lineId = in.readLong();
            version = in.readInt();
            short numSharers = in.readShort();
            sharers = new HashSet<>(numSharers);
            for (int i = 0; i < numSharers; i++) {
                sharers.add(in.readShort());
            }
            int bytesToRead = in.readInt();
            data = in.readByteBuf(bytesToRead);
        }

        @Override
        Message copy() {
            throw new NotImplementedException();
        }
    }

    static class INV extends Request {
        long lineId;

        INV() {
            super(MessageType.INVALIDATE, (short)-1);
        }

        INV(long lineId, short nodeId) {
            super(MessageType.INVALIDATE, nodeId);
            this.lineId = lineId;
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize()
                    + 8 // line id long
                    ;
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
        Message copy() {
            return new INV(lineId, super.sender);
        }
    }

    static class INVACK extends Response {
        long lineId;

        INVACK() {
            super(MessageType.INVALIDATE_ACK);
        }

        INVACK(long lineId, Request inResponseTo, short sender) {
            super(MessageType.INVALIDATE_ACK, inResponseTo, sender);
            this.lineId = lineId;
        }

        @Override
        int calcByteSize() {
            return super.calcByteSize()
                    + 8 // line id long
                    ;
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
        Message copy() {
            throw new NotImplementedException();
        }
    }

}

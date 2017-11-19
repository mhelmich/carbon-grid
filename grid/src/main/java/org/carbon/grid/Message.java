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
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Set;

abstract class Message implements Persistable {

    final MessageType type;
    protected int messageSequenceNumber;
    protected short sender;
    protected long lineId;

    Message(MessageType type) {
        this.type = type;
    }

    @Override
    public void write(MessageOutput out) throws IOException {
        out.writeByte(type.ordinal);
        out.writeInt(messageSequenceNumber);
        out.writeShort(sender);
        out.writeLong(lineId);
    }

    @Override
    public void read(MessageInput in) throws IOException {
        messageSequenceNumber = in.readInt();
        sender = in.readShort();
        lineId = in.readLong();
    }

    @Override
    public int byteSize() {
        return calcMessagesByteSize();
    }

    protected int calcMessagesByteSize() {
        return 1     // message type byte
             + 4     // message id int
             + 2     // sender short
             + 8     // line id long
             ;
    }

    Message copy() {
        throw new NotImplementedException();
    }

    int getMessageSequenceNumber() {
        return messageSequenceNumber;
    }

    void setMessageSequenceNumber(int messageSequenceNumber) {
        this.messageSequenceNumber = messageSequenceNumber;
    }

    short getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "message type: " + type + " messageSequenceNumber: " + messageSequenceNumber + " sender: " + sender + " lineId: " + lineId;
    }

    static abstract class Request extends Message {
        Request(MessageType type) {
            super(type);
        }
    }

    static abstract class Response extends Message {
        Response(MessageType type) {
            super(type);
        }
    }

    static class GET extends Request {
        private GET() {
            super(MessageType.GET);
        }

        GET(short sender, long lineId) {
            super(MessageType.GET);
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        GET copy() {
            return new GET(sender, lineId);
        }
    }

    static class PUT extends Response {
        int version;
        ByteBuf data;

        private PUT() {
            super(MessageType.PUT);
        }

        PUT(int requestMessageSequenceNumber, short sender, long lineId, int version, ByteBuf data) {
            super(MessageType.PUT);
            this.messageSequenceNumber = requestMessageSequenceNumber;
            this.sender = sender;
            this.lineId = lineId;
            this.version = version;
            this.data = data;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 4                 // version number int
                    + 4                 // bytebuf size
                    + data.capacity()   // buffer content
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeInt(version);
            out.writeInt(data.capacity());
            out.writeByteBuf(data.resetReaderIndex());
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            version = in.readInt();
            int bytesToRead = in.readInt();
            data = in.readByteBuf(bytesToRead);
        }
    }

    static class ACK extends Response {
        private ACK() {
            super(MessageType.ACK);
        }

        ACK(int requestMessageSequenceNumber, short sender, long lineId) {
            super(MessageType.ACK);
            this.messageSequenceNumber = requestMessageSequenceNumber;
            this.sender = sender;
            this.lineId = lineId;
        }
    }

    static class GETX extends Request {
        private GETX() {
            super(MessageType.GETX);
        }

        GETX(short sender, long lineId) {
            super(MessageType.GETX);
            this.sender = sender;
            this.lineId = lineId;
        }

        @Override
        GETX copy() {
            return new GETX(sender, lineId);
        }
    }

    static class PUTX extends Response {
        int version;
        Set<Short> sharers;
        ByteBuf data;

        private PUTX() {
            super(MessageType.PUTX);
        }

        PUTX(int requestMessageSequenceNumber, short sender, long lineId, int version, Set<Short> sharers, ByteBuf data) {
            super(MessageType.PUTX);
            this.messageSequenceNumber = requestMessageSequenceNumber;
            this.sender = sender;
            this.lineId = lineId;
            this.version = version;
            this.sharers = sharers;
            this.data = data;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 4                    // version number int
                    + 2                    // num sharers short
                    + (2 * sharers.size()) // all sharers short
                    + 4                    // bytebuf size
                    + data.capacity()      // buffer content
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
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
            version = in.readInt();
            short numSharers = in.readShort();
            sharers = new NonBlockingHashSet<>();
            for (int i = 0; i < numSharers; i++) {
                sharers.add(in.readShort());
            }
            int bytesToRead = in.readInt();
            data = in.readByteBuf(bytesToRead);
        }
    }

    static class OWNER_CHANGED extends Response {
        short newOwner;

        private OWNER_CHANGED() {
            super(MessageType.OWNER_CHANGED);
        }

        OWNER_CHANGED(int requestMessageId, short sender, long lineId, short newOwner) {
            super(MessageType.OWNER_CHANGED);
            this.messageSequenceNumber = requestMessageId;
            this.sender = sender;
            this.lineId = lineId;
            this.newOwner = newOwner;
        }

        @Override
        public int calcMessagesByteSize() {
            return super.calcMessagesByteSize()
                    + 2       // new owner short
                    ;
        }

        @Override
        public void write(MessageOutput out) throws IOException {
            super.write(out);
            out.writeShort(newOwner);
        }

        @Override
        public void read(MessageInput in) throws IOException {
            super.read(in);
            newOwner = in.readShort();
        }
    }

    static class INV extends Request {
        private INV() {
            super(MessageType.INV);
        }
    }

    static class INVACK extends Response {
        private INVACK() {
            super(MessageType.INVACK);
        }

        INVACK(int requestMessageId, short sender, long lineId) {
            super(MessageType.INVACK);
            this.messageSequenceNumber = requestMessageId;
            this.sender = sender;
            this.lineId = lineId;
        }
    }

    static class DeserializationMessageFactory {
        Message createMessageShellForType(MessageType type) {
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
                case OWNER_CHANGED:
                    return new OWNER_CHANGED();
                case INV:
                    return new INV();
                case INVACK:
                    return new INVACK();
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }
    }
}

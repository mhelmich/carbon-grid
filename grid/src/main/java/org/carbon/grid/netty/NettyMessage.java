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

package org.carbon.grid.netty;

import io.netty.buffer.ByteBuf;
import org.carbon.grid.MessageInput;
import org.carbon.grid.MessageOutput;
import org.carbon.grid.Persistable;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Set;

abstract class NettyMessage implements Persistable {

    final NettyMessageType type;
    protected int messageSequenceNumber;
    protected short sender;
    protected long lineId;

    NettyMessage(NettyMessageType type) {
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

    NettyMessage copy() {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        return "message type: " + type + " messageSequenceNumber: " + messageSequenceNumber + " sender: " + sender + " lineId: " + lineId;
    }

    static abstract class Request extends NettyMessage {
        Request(NettyMessageType type) {
            super(type);
        }
    }

    static abstract class Response extends NettyMessage {
        Response(NettyMessageType type) {
            super(type);
        }
    }

    static class GET extends Request {
        @Deprecated
        GET() {
            super(NettyMessageType.GET);
        }

        GET(short sender, long lineId) {
            super(NettyMessageType.GET);
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

        @Deprecated
        PUT() {
            super(NettyMessageType.PUT);
        }

        PUT(int requestMessageSequenceNumber, short sender, long lineId, int version, ByteBuf data) {
            super(NettyMessageType.PUT);
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
                    + data.capacity();  // buffer content
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
        @Deprecated
        ACK() {
            super(NettyMessageType.ACK);
        }

        ACK(int requestMessageSequenceNumber, short sender, long lineId) {
            super(NettyMessageType.ACK);
            this.messageSequenceNumber = requestMessageSequenceNumber;
            this.sender = sender;
            this.lineId = lineId;
        }
    }

    static class GETX extends Request {
        @Deprecated
        GETX() {
            super(NettyMessageType.GETX);
        }

        GETX(short sender, long lineId) {
            super(NettyMessageType.GETX);
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

        @Deprecated
        PUTX() {
            super(NettyMessageType.PUTX);
        }

        PUTX(int requestMessageSequenceNumber, short sender, long lineId, int version, Set<Short> sharers, ByteBuf data) {
            super(NettyMessageType.PUTX);
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
                    + data.capacity();     // buffer content
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

    @SuppressWarnings("deprecation")
    static class OWNER_CHANGED extends Response {
        short newOwner;

        @Deprecated
        OWNER_CHANGED() {
            super(NettyMessageType.OWNER_CHANGED);
        }

        OWNER_CHANGED(int requestMessageId, short sender, long lineId, short newOwner) {
            super(NettyMessageType.OWNER_CHANGED);
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
}
